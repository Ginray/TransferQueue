#!/usr/bin/env python3
"""Exercise UCX control-state ownership and cancellation."""

from __future__ import annotations

import argparse
import asyncio
import os
from uuid import uuid4

import ray
import torch
import zmq
import zmq.asyncio
from test_simplestorage_ucx_integration import make_manager

from transfer_queue.storage.data_plane import address_digest
from transfer_queue.storage.simple_storage import SimpleStorageUnit
from transfer_queue.utils.zmq_utils import ZMQMessage, ZMQRequestType, create_zmq_socket


async def request(info, message: ZMQMessage) -> ZMQMessage:
    context = zmq.asyncio.Context()
    socket = create_zmq_socket(
        context,
        zmq.DEALER,
        info.ip,
        identity=f"protocol-state-{uuid4().hex[:8]}".encode(),
    )
    socket.setsockopt(zmq.RCVTIMEO, 10_000)
    socket.setsockopt(zmq.SNDTIMEO, 10_000)
    socket.connect(info.to_addr("put_get_socket"))
    try:
        await socket.send_multipart(message.serialize(), copy=False)
        return ZMQMessage.deserialize(await socket.recv_multipart(copy=False))
    finally:
        socket.close(linger=0)
        context.term()


def control_message(
    request_type: ZMQRequestType,
    sender_id: str,
    receiver_id: str,
    body: dict,
) -> ZMQMessage:
    return ZMQMessage.create(
        request_type=request_type,
        sender_id=sender_id,
        receiver_id=receiver_id,
        body=body,
    )


async def run_case(resource: str | None) -> None:
    options = {"num_cpus": 1}
    if resource:
        options["resources"] = {resource: 1}
    actor = SimpleStorageUnit.options(**options).remote(
        storage_unit_size=None,
        payload_transport_enabled=True,
    )
    manager = None
    try:
        info = ray.get(actor.get_zmq_server_info.remote())
        data_info = ray.get(actor.get_payload_transport_info.remote())
        manager = make_manager(info, data_info, True, 64 * 1024)

        # Prepare a GET without posting the Manager receive or committing it,
        # then cancel it through the production dedicated cancellation socket.
        value = torch.arange(1_000_000, dtype=torch.int64)
        await manager._put_to_single_storage_unit([700], {"tensor": [value]}, target_storage_unit=info.id)
        get_descriptor = manager._new_descriptor(0, 0)
        receiver_address = manager.data_plane.address
        response = await request(
            info,
            control_message(
                ZMQRequestType.GET_DATA_PREPARE,
                manager.storage_manager_id,
                info.id,
                {
                    "global_indexes": [700],
                    "fields": ["tensor"],
                    "descriptor": get_descriptor.to_dict(),
                    "receiver_address": receiver_address,
                    "receiver_address_digest": address_digest(receiver_address),
                },
            ),
        )
        assert response.request_type == ZMQRequestType.GET_DATA_READY
        assert ray.get(actor.get_data_plane_pending_counts.remote())["pending_gets"] == 1
        response = await request(
            info,
            control_message(
                ZMQRequestType.GET_DATA_CANCEL,
                "intruder-B",
                info.id,
                {"transfer_id": get_descriptor.transfer_id},
            ),
        )
        assert response.request_type == ZMQRequestType.PUT_GET_ERROR
        assert ray.get(actor.get_data_plane_pending_counts.remote())["pending_gets"] == 1
        await manager._cancel_ucx_get(get_descriptor.transfer_id, info.id)
        assert ray.get(actor.get_data_plane_pending_counts.remote())["pending_gets"] == 0
        print("GET cancel PASS", flush=True)

        # Bind a PUT state to its logical sender. A different sender must not
        # cancel the posted receive.
        put_descriptor = manager._new_descriptor(1024, 1)
        prepare_body = {
            "global_indexes": [701],
            "descriptor": put_descriptor.to_dict(),
            "data_parser": None,
        }
        response = await request(
            info,
            control_message(ZMQRequestType.PUT_DATA_PREPARE, "owner-A", info.id, prepare_body),
        )
        assert response.request_type == ZMQRequestType.PUT_DATA_READY
        response = await request(
            info,
            control_message(
                ZMQRequestType.PUT_DATA_CANCEL,
                "intruder-B",
                info.id,
                {"transfer_id": put_descriptor.transfer_id},
            ),
        )
        assert response.request_type == ZMQRequestType.PUT_GET_ERROR
        assert ray.get(actor.get_data_plane_pending_counts.remote())["pending_puts"] == 1
        response = await request(
            info,
            control_message(
                ZMQRequestType.PUT_DATA_CANCEL,
                "owner-A",
                info.id,
                {"transfer_id": put_descriptor.transfer_id},
            ),
        )
        assert response.request_type == ZMQRequestType.PUT_DATA_RESPONSE
        assert ray.get(actor.get_data_plane_pending_counts.remote())["pending_puts"] == 0
        print("PUT sender binding PASS", flush=True)

    finally:
        if manager is not None:
            manager.close()
        ray.kill(actor)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--resource", default=None)
    args = parser.parse_args()
    runtime_env_vars = {
        "TORCH_DEVICE_BACKEND_AUTOLOAD": "0",
        "TQ_STORAGE_POLLER_TIMEOUT": "1",
    }
    for name in ("TQ_RAY_WORKER_PYTHONPATH", "TQ_RAY_WORKER_LD_LIBRARY_PATH"):
        value = os.environ.get(name)
        if value:
            runtime_env_vars[name.removeprefix("TQ_RAY_WORKER_")] = value
    ray.init(
        address=args.ray_address,
        include_dashboard=False,
        runtime_env={"env_vars": runtime_env_vars},
    )
    try:
        asyncio.run(run_case(args.resource))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
