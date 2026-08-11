#!/usr/bin/env python3
"""Minimal raw UCP Tagged host test without torch/TQ imports."""

from __future__ import annotations

import argparse
import base64
import hashlib
import time
from pathlib import Path

import _ucx

TRANSFER_ID = "tq-raw-host-transfer"


def tag() -> int:
    return int.from_bytes(hashlib.blake2b(TRANSFER_ID.encode(), digest_size=8).digest(), "big") & ((1 << 63) - 1)


def payload(size: int) -> bytes:
    seed = hashlib.sha256(TRANSFER_ID.encode()).digest()
    return (seed * ((size // len(seed)) + 1))[:size]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("server", "client"))
    parser.add_argument("address_file", type=Path)
    parser.add_argument("--size", type=int, default=16 * 1024 * 1024)
    args = parser.parse_args()
    worker = _ucx.Worker()
    data = payload(args.size)
    try:
        if args.role == "server":
            args.address_file.write_text(base64.b64encode(worker.address()).decode())
            request = worker.post_receive(tag(), args.size)
            received = request.wait(60)
            assert received == data
            print(f"raw server PASS size={args.size}", flush=True)
        else:
            deadline = time.monotonic() + 30
            while not args.address_file.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("address file missing")
                time.sleep(0.1)
            address = base64.b64decode(args.address_file.read_text())
            endpoint = worker.connect(address)
            request = endpoint.post_send(tag(), data)
            request.wait(60)
            print(f"raw client PASS size={args.size}", flush=True)
    finally:
        worker.close()


if __name__ == "__main__":
    main()
