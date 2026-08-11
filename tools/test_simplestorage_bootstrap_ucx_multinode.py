#!/usr/bin/env python3
"""Validate formal TQ bootstrap with one UCX StorageUnit on each Ray node."""

from __future__ import annotations

import asyncio
import os

import ray
import torch
from omegaconf import OmegaConf

import transfer_queue as tq


async def roundtrip_all_units() -> None:
    manager = tq.get_client().storage_manager
    assert manager.data_plane is not None
    assert len(manager.storage_unit_infos) == 2
    assert set(manager.payload_transport_infos) == set(manager.storage_unit_infos)

    for offset, (storage_id, storage_info) in enumerate(manager.storage_unit_infos.items()):
        data_info = manager.payload_transport_infos[storage_id]
        print(
            f"bootstrap unit metadata PASS id={storage_id} host={storage_info.ip} "
            f"address_len={len(data_info['address'])}",
            flush=True,
        )
        value = torch.arange(1_000_000, dtype=torch.int64) + offset
        index = [700 + offset]
        await manager._put_to_single_storage_unit(
            index, {"tensor": [value]}, target_storage_unit=storage_id
        )
        _, result = await manager._get_from_single_storage_unit(
            index, ["tensor"], target_storage_unit=storage_id
        )
        assert torch.equal(result["tensor"][0], value)
        print(f"bootstrap unit roundtrip PASS id={storage_id} host={storage_info.ip}", flush=True)


def main() -> None:
    conf = OmegaConf.create(
        {
            "controller": {"polling_mode": True},
            "backend": {
                "storage_backend": "SimpleStorage",
                "SimpleStorage": {
                    "total_storage_size": 128 * 1024 * 1024,
                    "num_data_storage_units": 2,
                    "payload_transport": {"enabled": True},
                },
            },
        },
        flags={"allow_objects": True},
    )
    ray.init(address=os.environ["RAY_ADDRESS"], include_dashboard=False)
    try:
        tq.init(conf)
        asyncio.run(roundtrip_all_units())
        print("bootstrap multinode PASS", flush=True)
        tq.close()
        print("bootstrap multinode close PASS", flush=True)
        tq.init(conf)
        asyncio.run(roundtrip_all_units())
        print("bootstrap multinode replacement PASS", flush=True)
    finally:
        tq.close()
        ray.shutdown()


if __name__ == "__main__":
    main()
