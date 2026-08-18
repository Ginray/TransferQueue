#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import time
from pathlib import Path

from transfer_queue.storage.payload_transfer.ucx_runtime import (
    UcxRuntime,
    UcxTransfer,
    address_digest,
    create_ucx_runtime,
    transfer_tag,
)

TRANSFER_ID = "tq-standalone-data-plane"


def payload(size: int) -> bytes:
    seed = hashlib.sha256(TRANSFER_ID.encode()).digest()
    return (seed * ((size // len(seed)) + 1))[:size]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("role", choices=("server", "client", "server_exit", "peer_exit"))
    p.add_argument("address_file", type=Path)
    p.add_argument("--size", type=int, default=8 * 1024 * 1024)
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--local-ip", help="use TQ UCX device and transport discovery")
    args = p.parse_args()
    if args.local_ip:
        dp = create_ucx_runtime(local_ip=args.local_ip)
    else:
        dp = UcxRuntime(args.timeout)
    descriptor = UcxTransfer(TRANSFER_ID, transfer_tag(TRANSFER_ID), args.size)
    try:
        if args.role == "server":
            dp.prepare_receive(descriptor)
            args.address_file.write_text(base64.b64encode(dp.address).decode())
            actual = dp.finish_receive(descriptor)
            assert actual == payload(args.size)
            print("payload_transfer server PASS", flush=True)
        elif args.role == "server_exit":
            args.address_file.write_text(base64.b64encode(dp.address).decode())
            print("payload_transfer server_exit READY", flush=True)
        elif args.role == "client":
            deadline = time.monotonic() + 30
            while not args.address_file.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("address file missing")
                time.sleep(0.1)
            peer = base64.b64decode(args.address_file.read_text())
            dp.send(peer, descriptor, payload(args.size), peer_address_digest=address_digest(peer))
            print("payload_transfer client PASS", flush=True)
        else:
            deadline = time.monotonic() + 30
            while not args.address_file.exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("address file missing")
                time.sleep(0.1)
            try:
                peer = base64.b64decode(args.address_file.read_text())
                dp.send(peer, descriptor, payload(args.size), peer_address_digest=address_digest(peer))
            except (RuntimeError, TimeoutError):
                print("payload_transfer peer_exit PASS", flush=True)
            else:
                raise AssertionError("send unexpectedly succeeded after peer exit")
    finally:
        dp.close()


if __name__ == "__main__":
    main()
