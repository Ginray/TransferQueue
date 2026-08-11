#!/usr/bin/env python3
"""Exercise the production TQ bootstrap and manager wiring with UCX enabled."""

from __future__ import annotations

import asyncio
import os

import ray
import torch
from omegaconf import OmegaConf

import transfer_queue as tq


async def run_roundtrip(label: str, index: int) -> None:
    client = tq.get_client()
    manager = client.storage_manager
    assert manager.data_plane is not None
    assert manager.payload_transport_infos
    storage_id, storage_info = next(iter(manager.storage_unit_infos.items()))
    assert manager.payload_transport_infos[storage_id]["provider"] == "ucx"
    print(
        f"bootstrap metadata PASS storage={storage_id} host={storage_info.ip} "
        f"data_address_len={len(manager.payload_transport_infos[storage_id]['address'])}",
        flush=True,
    )

    value = torch.arange(1_000_000, dtype=torch.int64)
    await manager._put_to_single_storage_unit(
        [index], {"tensor": [value]}, target_storage_unit=storage_id
    )
    _, result = await manager._get_from_single_storage_unit(
        [index], ["tensor"], target_storage_unit=storage_id
    )
    assert torch.equal(result["tensor"][0], value)
    print(f"{label} manager roundtrip PASS", flush=True)


def main() -> None:
    conf = OmegaConf.create(
        {
            "controller": {"polling_mode": True},
            "backend": {
                "storage_backend": "SimpleStorage",
                "SimpleStorage": {
                    "total_storage_size": 64 * 1024 * 1024,
                    "num_data_storage_units": 1,
                    "payload_transport": {"enabled": True},
                },
            },
        },
        flags={"allow_objects": True},
    )
    ray.init(address=os.environ["RAY_ADDRESS"], include_dashboard=False)
    try:
        final_conf = tq.init(conf)
        assert final_conf.backend.SimpleStorage.payload_transport_infos
        asyncio.run(run_roundtrip("bootstrap initial", 301))
        tq.close()
        print("controller close PASS", flush=True)

        final_conf = tq.init(conf)
        assert final_conf.backend.SimpleStorage.payload_transport_infos
        asyncio.run(run_roundtrip("bootstrap replacement", 302))
        print("controller replacement PASS", flush=True)
    finally:
        tq.close()
        ray.shutdown()


if __name__ == "__main__":
    main()
