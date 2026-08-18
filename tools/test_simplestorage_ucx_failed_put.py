#!/usr/bin/env python3
"""Verify that a failed UCX PUT does not commit a partial object."""

from __future__ import annotations

import argparse
import asyncio
import os
from unittest.mock import PropertyMock, patch

import ray
import torch
from test_simplestorage_ucx_integration import make_manager

from transfer_queue.storage.payload_transfer.ucx_runtime import UcxRuntime, address_digest
from transfer_queue.storage.simple_storage import SimpleStorageUnit

THRESHOLD = 64 * 1024


async def run_case(resource: str | None) -> None:
    options = {"num_cpus": 1}
    if resource:
        options["resources"] = {resource: 1}
    actor = SimpleStorageUnit.options(**options).remote(
        storage_unit_size=None,
        payload_transfer="ucx",
    )
    manager = None
    dead_peer = None
    try:
        info = ray.get(actor.get_zmq_server_info.remote())
        data_info = ray.get(actor.get_payload_transfer_info.remote())
        manager = make_manager(info, data_info, True, THRESHOLD)
        value = torch.arange(1_000_000, dtype=torch.int64)
        endpoint_data = manager.payload_transfer_infos[info.id]["endpoint"]["data"]
        valid_address = endpoint_data["address"]

        # Production bootstrap metadata must reject a same-length mutation
        # before ucp_ep_create().  Keep the original digest intentionally.
        corrupt_address = bytearray(valid_address)
        corrupt_address[0] ^= 0xFF
        endpoint_data["address"] = bytes(corrupt_address)
        try:
            await manager._put_to_single_storage_unit([500], {"tensor": [value]}, target_storage_unit=info.id)
        except Exception as exc:
            print(f"corrupt bootstrap address expected error={type(exc).__name__}", flush=True)
        else:
            raise AssertionError("corrupt bootstrap address unexpectedly succeeded")
        pending = ray.get(actor.get_payload_transfer_pending_counts.remote())
        assert pending == {"pending_puts": 0, "pending_gets": 0, "pending_receives": 0}, pending
        print(f"corrupt bootstrap address guard PASS state={pending}", flush=True)
        endpoint_data["address"] = valid_address

        dead_peer = UcxRuntime(10)
        dead_address = dead_peer.address
        dead_peer.close()
        endpoint_data["address"] = dead_address
        endpoint_data["address_digest"] = address_digest(dead_address)
        try:
            await manager._put_to_single_storage_unit([501], {"tensor": [value]}, target_storage_unit=info.id)
        except Exception as exc:
            print(f"failed PUT expected error={type(exc).__name__}", flush=True)
        else:
            raise AssertionError("failed UCX PUT unexpectedly succeeded")

        pending = ray.get(actor.get_payload_transfer_pending_counts.remote())
        assert pending == {"pending_puts": 0, "pending_gets": 0, "pending_receives": 0}, pending
        print(f"failed PUT remote cleanup PASS state={pending}", flush=True)

        endpoint_data["address"] = valid_address
        endpoint_data["address_digest"] = address_digest(valid_address)
        try:
            await manager._get_from_single_storage_unit([501], ["tensor"], target_storage_unit=info.id)
        except Exception:
            print("failed PUT not committed PASS", flush=True)
        else:
            raise AssertionError("failed UCX PUT left a readable object")

        await manager._put_to_single_storage_unit([502], {"tensor": [value + 1]}, target_storage_unit=info.id)

        # Exercise the reverse GET direction with an address altered in the
        # control message while retaining the original digest.
        receiver_address = manager.payload_transfer.endpoint().data["address"]
        corrupt_receiver = bytearray(receiver_address)
        corrupt_receiver[0] ^= 0xFF
        with (
            patch.object(UcxRuntime, "address", new_callable=PropertyMock, return_value=bytes(corrupt_receiver)),
            patch(
                "transfer_queue.storage.payload_transfer.ucx.address_digest",
                return_value=address_digest(receiver_address),
            ),
        ):
            try:
                await manager._get_from_single_storage_unit([502], ["tensor"], target_storage_unit=info.id)
            except Exception as exc:
                print(f"corrupt GET receiver expected error={type(exc).__name__}", flush=True)
            else:
                raise AssertionError("corrupt GET receiver unexpectedly succeeded")
        pending = ray.get(actor.get_payload_transfer_pending_counts.remote())
        assert pending == {"pending_puts": 0, "pending_gets": 0, "pending_receives": 0}, pending
        print(f"corrupt GET receiver guard PASS state={pending}", flush=True)

        _, result = await manager._get_from_single_storage_unit([502], ["tensor"], target_storage_unit=info.id)
        assert torch.equal(result["tensor"][0], value + 1)
        print("subsequent valid PUT/GET PASS", flush=True)
    finally:
        if manager is not None and manager.payload_transfer is not None:
            manager.payload_transfer.close()
        if dead_peer is not None:
            dead_peer.close()
        ray.kill(actor)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--resource", default=None)
    args = parser.parse_args()
    runtime_env_vars = {
        "TORCH_DEVICE_BACKEND_AUTOLOAD": "0",
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
