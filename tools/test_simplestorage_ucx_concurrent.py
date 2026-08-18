#!/usr/bin/env python3
"""Concurrent SimpleStorage UCX PUT/GET validation on one remote actor."""

from __future__ import annotations

import argparse
import asyncio
import os
import time

import ray
import torch
from test_simplestorage_ucx_integration import make_manager

from transfer_queue.storage.managers.simple_storage_manager import AsyncSimpleStorageManager
from transfer_queue.storage.simple_storage import SimpleStorageUnit


async def main_case(concurrency: int, elements: int, resource: str | None, enabled: bool) -> None:
    threshold = 64 * 1024
    actor_options = {"num_cpus": 1}
    if resource:
        actor_options["resources"] = {resource: 1}
    actor = SimpleStorageUnit.options(**actor_options).remote(
        storage_unit_size=None,
        payload_transfer="ucx" if enabled else "zmq",
    )
    info = ray.get(actor.get_zmq_server_info.remote())
    data_info = ray.get(actor.get_payload_transfer_info.remote())
    managers: list[AsyncSimpleStorageManager] = []
    try:
        values = [torch.arange(elements, dtype=torch.int64) + i for i in range(concurrency)]
        for i in range(concurrency):
            manager = make_manager(info, data_info, enabled, threshold)
            manager.storage_manager_id = f"TQ_CONCURRENT_MANAGER_{i}"
            managers.append(manager)

        async def put_one(i: int) -> None:
            await managers[i]._put_to_single_storage_unit(
                [10_000 + i], {"tensor": [values[i]]}, target_storage_unit=info.id
            )

        start = time.perf_counter()
        await asyncio.gather(*(put_one(i) for i in range(concurrency)))
        put_seconds = time.perf_counter() - start

        async def get_one(i: int) -> None:
            _, result = await managers[i]._get_from_single_storage_unit(
                [10_000 + i], ["tensor"], target_storage_unit=info.id
            )
            assert torch.equal(result["tensor"][0], values[i])

        start = time.perf_counter()
        await asyncio.gather(*(get_one(i) for i in range(concurrency)))
        get_seconds = time.perf_counter() - start
        total_bytes = concurrency * elements * 8
        print(
            f"concurrent PASS mode={'ucx' if enabled else 'legacy'} "
            f"concurrency={concurrency} elements={elements} "
            f"put_seconds={put_seconds:.6f} get_seconds={get_seconds:.6f} "
            f"put_mib_s={total_bytes / put_seconds / 2**20:.2f} "
            f"get_mib_s={total_bytes / get_seconds / 2**20:.2f}",
            flush=True,
        )
    finally:
        for manager in managers:
            if manager.payload_transfer is not None:
                manager.payload_transfer.close()
        ray.kill(actor)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ray-address", required=True)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--elements", type=int, default=1_000_000)
    parser.add_argument("--resource", default=None)
    parser.add_argument("--mode", choices=("legacy", "ucx"), default="ucx")
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
        runtime_env={"env_vars": runtime_env_vars} if runtime_env_vars else None,
    )
    try:
        asyncio.run(
            main_case(
                args.concurrency,
                args.elements,
                args.resource,
                enabled=args.mode == "ucx",
            )
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
