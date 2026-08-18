#!/usr/bin/env python3
"""Check same-process TQ re-entry after the controller actor is killed."""

from __future__ import annotations

import os

import ray
from omegaconf import OmegaConf

import transfer_queue as tq
from transfer_queue import interface


def config():
    return OmegaConf.create(
        {
            "controller": {"polling_mode": True},
            "backend": {
                "storage_backend": "SimpleStorage",
                "SimpleStorage": {
                    "total_storage_size": 64,
                    "num_data_storage_units": 1,
                    "payload_transfer": "zmq",
                },
            },
        },
        flags={"allow_objects": True},
    )


def main() -> None:
    ray.init(address=os.environ["RAY_ADDRESS"], include_dashboard=False)
    try:
        tq.init(config())
        controller = interface._TQ_CONTROLLER
        assert controller is not None
        print("controller initial init PASS", flush=True)

        ray.kill(controller)
        try:
            ray.get(controller.get_config.remote(), timeout=2)
        except Exception:
            print("controller crash observed PASS", flush=True)
        else:
            raise AssertionError("controller remained available after ray.kill")

        try:
            tq.init(config())
        except Exception as exc:
            print(f"same-process tq.init after crash expected error={type(exc).__name__}", flush=True)
        else:
            raise AssertionError("same-process tq.init unexpectedly recovered a dead controller")
    finally:
        tq.close()
        ray.shutdown()


if __name__ == "__main__":
    main()
