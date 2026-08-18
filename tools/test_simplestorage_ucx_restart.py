#!/usr/bin/env python3
"""Verify SimpleStorage UCX recovery after replacing the StorageUnit actor."""

from __future__ import annotations

import argparse
import asyncio
import os

import ray
import torch
from test_simplestorage_ucx_integration import make_manager

from transfer_queue.storage.simple_storage import SimpleStorageUnit

THRESHOLD = 64 * 1024


def actor_options(resource: str | None) -> dict:
    options = {"num_cpus": 1}
    if resource:
        options["resources"] = {resource: 1}
    return options


def create_actor(resource: str | None):
    return SimpleStorageUnit.options(**actor_options(resource)).remote(
        storage_unit_size=None,
        payload_transfer="ucx",
    )


async def roundtrip(manager, info, index: int, value: torch.Tensor) -> None:
    await manager._put_to_single_storage_unit([index], {"tensor": [value]}, target_storage_unit=info.id)
    _, result = await manager._get_from_single_storage_unit([index], ["tensor"], target_storage_unit=info.id)
    assert torch.equal(result["tensor"][0], value)


async def run_case(resource: str | None) -> None:
    actor = create_actor(resource)
    manager = None
    try:
        info = ray.get(actor.get_zmq_server_info.remote())
        data_info = ray.get(actor.get_payload_transfer_info.remote())
        manager = make_manager(info, data_info, True, THRESHOLD)
        await roundtrip(manager, info, 201, torch.arange(1_000_000, dtype=torch.int64))
        print(f"restart initial PASS actor={info.id} host={info.ip}", flush=True)
        manager.payload_transfer.close()
        manager = None

        ray.kill(actor)
        actor = create_actor(resource)
        new_info = ray.get(actor.get_zmq_server_info.remote())
        new_data_info = ray.get(actor.get_payload_transfer_info.remote())
        manager = make_manager(new_info, new_data_info, True, THRESHOLD)
        await roundtrip(manager, new_info, 202, torch.arange(1_000_000, dtype=torch.int64) + 1)
        print(f"restart replacement PASS actor={new_info.id} host={new_info.ip}", flush=True)
    finally:
        if manager is not None and manager.payload_transfer is not None:
            manager.payload_transfer.close()
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
