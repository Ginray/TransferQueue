#!/usr/bin/env python3
"""Verify that the native Tagged binding accepts a retained bytearray buffer."""

from __future__ import annotations

import argparse
import base64
import hashlib
import time
from pathlib import Path

from transfer_queue import _ucx

SIZE = 8 * 1024 * 1024
TAG = 0x544251


def payload() -> bytes:
    seed = hashlib.sha256(b"tq-bytearray-binding").digest()
    return (seed * ((SIZE // len(seed)) + 1))[:SIZE]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("server", "client"))
    parser.add_argument("address_file", type=Path)
    args = parser.parse_args()
    if args.role == "server":
        worker = _ucx.Worker()
        args.address_file.write_text(base64.b64encode(worker.address()).decode())
        try:
            request = worker.post_receive(TAG, SIZE)
            actual = bytes(request.wait(30.0))
            assert actual == payload()
            print("bytearray server PASS", flush=True)
        finally:
            worker.close()
        return

    deadline = time.monotonic() + 30
    while not args.address_file.exists():
        if time.monotonic() > deadline:
            raise TimeoutError("server address file was not created")
        time.sleep(0.1)
    worker = _ucx.Worker()
    endpoint = worker.connect(base64.b64decode(args.address_file.read_text()))
    try:
        request = endpoint.post_send(TAG, bytearray(payload()))
        request.wait(30.0)
        print("bytearray client PASS", flush=True)
    finally:
        endpoint.close(30.0)
        worker.close()


if __name__ == "__main__":
    main()
