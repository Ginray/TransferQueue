#!/usr/bin/env python3
"""Check the behavior of an existing Manager after the controller actor dies."""

from __future__ import annotations

import asyncio
import os

import ray
import torch
from omegaconf import OmegaConf

import transfer_queue as tq
from transfer_queue import interface


def make_config():
    return OmegaConf.create(
        {
            "controller": {"polling_mode": True},
            "backend": {
                "storage_backend": "SimpleStorage",
                "SimpleStorage": {
                    "total_storage_size": 64 * 1024 * 1024,
                    "num_data_storage_units": 1,
                    "payload_transfer": "ucx",
                },
            },
        },
        flags={"allow_objects": True},
    )


async def roundtrip(index: int) -> None:
    manager = tq.get_client().storage_manager
    storage_id = next(iter(manager.storage_unit_infos))
    value = torch.arange(1_000_000, dtype=torch.int64) + index
    await manager._put_to_single_storage_unit(
        [index], {"tensor": [value]}, target_storage_unit=storage_id
    )
    _, result = await manager._get_from_single_storage_unit(
        [index], ["tensor"], target_storage_unit=storage_id
    )
    assert torch.equal(result["tensor"][0], value)


async def observe_control_plane_failure() -> None:
    manager = tq.get_client().storage_manager
    try:
        await manager.notify_data_update(
            "controller_crash",
            [402],
            {"tensor": {"dtype": "int64", "shape": [1_000_000]}},
        )
    except (TimeoutError, RuntimeError) as exc:
        print(f"existing manager notify failure propagated PASS type={type(exc).__name__}", flush=True)
    else:
        raise AssertionError("notify_data_update unexpectedly succeeded without a controller")


def main() -> None:
    ray.init(address=os.environ["RAY_ADDRESS"], include_dashboard=False)
    try:
        tq.init(make_config())
        asyncio.run(roundtrip(401))
        controller = interface._TQ_CONTROLLER
        assert controller is not None
        ray.kill(controller)
        try:
            ray.get(controller.get_config.remote(), timeout=2)
        except Exception:
            print("controller unavailable PASS", flush=True)
        else:
            raise AssertionError("controller actor still answered after ray.kill")
        print("controller kill PASS", flush=True)

        # Keep the original Manager and StorageUnit; do not call tq.init/close here.
        asyncio.run(observe_control_plane_failure())
        asyncio.run(roundtrip(402))
        print("existing manager after controller crash PASS", flush=True)
    finally:
        tq.close()
        ray.shutdown()


if __name__ == "__main__":
    main()
