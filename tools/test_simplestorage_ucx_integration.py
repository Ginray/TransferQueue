#!/usr/bin/env python3
"""Minimal real SimpleStorage ZMQ/UCX integration test.

This intentionally bypasses the controller metadata path.  It exercises the
same manager methods and the real SimpleStorageUnit Ray actor, while keeping
the test focused on the storage data plane handshake.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time

import numpy as np
import ray
import torch
import zmq.asyncio
from tensordict import NonTensorStack
from tq_test_types import PickleValue

from transfer_queue.storage.managers.simple_storage_manager import AsyncSimpleStorageManager
from transfer_queue.storage.payload_transfer import create_payload_transfer
from transfer_queue.storage.simple_storage import SimpleStorageUnit


def parser(field_data):
    field_data["reference"] = [torch.full((3,), 7, dtype=torch.int64) for _ in field_data["reference"]]
    return field_data


def make_manager(
    server_info,
    data_info,
    enabled: bool,
    threshold: int,
):
    manager = object.__new__(AsyncSimpleStorageManager)
    manager.storage_manager_id = "TQ_INTEGRATION_MANAGER"
    # The focused integration tool bypasses StorageManager.__init__, but its
    # normal close/__del__ path still expects these base lifecycle fields.
    manager.controller_handshake_socket = None
    manager.zmq_context = zmq.asyncio.Context()
    manager.storage_unit_infos = {server_info.id: server_info}
    manager.payload_transfer = create_payload_transfer(
        "ucx" if enabled else "zmq", local_ip=ray.util.get_node_ip_address()
    )
    manager.payload_transfer_infos = {server_info.id: data_info} if enabled else {}
    manager.inline_threshold_bytes = threshold
    return manager


async def run_case(enabled: bool) -> None:
    threshold = 64 * 1024
    repeated_gets = max(1, int(os.environ.get("TQ_INTEGRATION_REPEATED_GETS", "1")))
    use_parser = os.environ.get("TQ_INTEGRATION_USE_PARSER", "1") == "1"
    actor_options = {"num_cpus": 1}
    actor_resource = os.environ.get("TQ_STORAGE_NODE_RESOURCE")
    if actor_resource:
        actor_options["resources"] = {actor_resource: 1}
    actor = SimpleStorageUnit.options(**actor_options).remote(
        storage_unit_size=None,
        payload_transfer="ucx" if enabled else "zmq",
    )
    info = ray.get(actor.get_zmq_server_info.remote())
    data_info = ray.get(actor.get_payload_transfer_info.remote())
    address = data_info["endpoint"]["data"]["address"] if data_info else None
    data_address_len = len(address) if address else None
    data_address_type = type(address).__name__ if address else None
    print(
        f"enabled={enabled} actor ready on {info.ip} data_address_len={data_address_len} type={data_address_type}",
        flush=True,
    )
    manager = make_manager(info, data_info, enabled, threshold)
    try:
        large_elements = int(os.environ.get("TQ_INTEGRATION_ELEMENTS", "1000000"))
        large = torch.arange(large_elements, dtype=torch.int64)
        values = {
            "tensor": [large],
            "numpy": [np.arange(32, dtype=np.float32)],
            "nested": [torch.nested.as_nested_tensor([torch.arange(3), torch.arange(5)], layout=torch.jagged)],
            "non_tensor_stack": [NonTensorStack("left", "right")],
            "pickle": [PickleValue("fallback")],
            "reference": ["shape:3"],
        }
        payload_bytes = large.numel() * large.element_size()
        repetitions = int(os.environ.get("TQ_INTEGRATION_REPETITIONS", "1"))
        for repetition in range(repetitions):
            key = 101 + repetition
            put_start = time.perf_counter()
            await manager._put_to_single_storage_unit(
                [key], values, target_storage_unit=info.id, data_parser=parser if use_parser else None
            )
            put_seconds = time.perf_counter() - put_start
            print(
                f"enabled={enabled} repetition={repetition} large PUT done seconds={put_seconds:.6f} "
                f"payload_bytes={payload_bytes} throughput_mib_s={payload_bytes / put_seconds / 2**20:.2f}",
                flush=True,
            )
            for get_repeat in range(repeated_gets):
                get_start = time.perf_counter()
                _, result = await manager._get_from_single_storage_unit(
                    [key], list(values), target_storage_unit=info.id
                )
                get_seconds = time.perf_counter() - get_start
                print(
                    f"enabled={enabled} repetition={repetition} get_repeat={get_repeat} "
                    f"large GET done seconds={get_seconds:.6f} payload_bytes={payload_bytes} "
                    f"throughput_mib_s={payload_bytes / get_seconds / 2**20}",
                    flush=True,
                )
        assert torch.equal(result["tensor"][0], large)
        np.testing.assert_array_equal(result["numpy"][0], values["numpy"][0])
        if use_parser:
            assert result["reference"][0].tolist() == [7, 7, 7]
        else:
            assert result["reference"][0] == "shape:3"
        assert isinstance(result["pickle"][0], PickleValue)
        assert result["pickle"][0].value == "fallback"

        # A small payload must use the legacy ZMQ path even when the UCX plane is enabled.
        small = {"small": [torch.tensor([1, 2, 3], dtype=torch.int64)]}
        await manager._put_to_single_storage_unit([102], small, target_storage_unit=info.id)
        print(f"enabled={enabled} small PUT done", flush=True)
        _, small_result = await manager._get_from_single_storage_unit([102], ["small"], target_storage_unit=info.id)
        print(f"enabled={enabled} small GET done", flush=True)
        assert torch.equal(small_result["small"][0], small["small"][0])

        await manager._clear_single_storage_unit(
            list(range(101, 101 + repetitions)) + [102], target_storage_unit=info.id
        )
        try:
            await manager._get_from_single_storage_unit([101 + repetitions], ["tensor"], target_storage_unit=info.id)
        except Exception:
            pass
        else:
            raise AssertionError("CLEAR did not remove stored data")
        print(f"simple_storage enabled={enabled} PASS", flush=True)
    finally:
        if manager.payload_transfer is not None:
            manager.payload_transfer.close()
        ray.kill(actor)


def main() -> None:
    parser_args = argparse.ArgumentParser()
    parser_args.add_argument("--mode", choices=("legacy", "ucx", "both"), default="both")
    parser_args.add_argument("--ray-address", default=None)
    args = parser_args.parse_args()
    runtime_env_vars = {}
    for name in ("TQ_RAY_WORKER_PYTHONPATH", "TQ_RAY_WORKER_LD_LIBRARY_PATH"):
        value = os.environ.get(name)
        if value:
            runtime_env_vars[name.removeprefix("TQ_RAY_WORKER_")] = value
    # Standard UCX diagnostics may be propagated by this validation tool; they
    # are not TransferQueue user configuration.
    for name in (
        "UCX_LOG_LEVEL",
        "UCX_LOG_FILE",
        "UCX_RNDV_SCHEME",
        "UCX_ZCOPY_THRESH",
        "UCX_RNDV_THRESH",
        "UCX_RNDV_FRAG_SIZE",
    ):
        value = os.environ.get(name)
        if value:
            runtime_env_vars[name] = value
    runtime_env = {"env_vars": runtime_env_vars} if runtime_env_vars else None
    ray.init(
        address=args.ray_address,
        ignore_reinit_error=True,
        include_dashboard=False,
        num_cpus=4 if args.ray_address is None else None,
        runtime_env=runtime_env,
    )
    modes = (False, True) if args.mode == "both" else (args.mode == "ucx",)
    try:
        asyncio.run(_run_modes(modes))
    finally:
        ray.shutdown()


async def _run_modes(modes):
    for enabled in modes:
        await run_case(enabled)


if __name__ == "__main__":
    main()
