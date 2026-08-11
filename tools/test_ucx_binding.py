#!/usr/bin/env python3
"""Small two-process test for the TQ native UCX Tagged binding."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import sys
import time
from pathlib import Path

from transfer_queue import _ucx

SIZES = (64 * 1024, 1024 * 1024, 16 * 1024 * 1024)
REPETITIONS = 10
REQUEST_TIMEOUT = float(os.environ.get("TQ_TEST_TIMEOUT", "30"))


def tag(size: int, iteration: int) -> int:
    return ((size << 16) ^ iteration) & ((1 << 63) - 1)


def payload(size: int, iteration: int) -> bytes:
    seed = hashlib.sha256(f"{size}:{iteration}".encode()).digest()
    return (seed * ((size // len(seed)) + 1))[:size]


def server(address_file: Path) -> None:
    worker = _ucx.Worker()
    address_file.write_text(base64.b64encode(worker.address()).decode())
    try:
        for size in SIZES:
            for iteration in range(REPETITIONS):
                expected = payload(size, iteration)
                request = worker.post_receive(tag(size, iteration), size)
                actual = bytes(request.wait(REQUEST_TIMEOUT))
                if actual != expected:
                    raise AssertionError(f"payload mismatch: size={size}, iteration={iteration}")
        print("server functional PASS", flush=True)
    finally:
        worker.close()


def client(address_file: Path) -> None:
    deadline = time.monotonic() + 30
    while not address_file.exists():
        if time.monotonic() > deadline:
            raise TimeoutError("server address file was not created")
        time.sleep(0.1)

    address = base64.b64decode(address_file.read_text())
    worker = _ucx.Worker()
    endpoint = worker.connect(address)
    try:
        for size in SIZES:
            for iteration in range(REPETITIONS):
                request = endpoint.post_send(tag(size, iteration), payload(size, iteration))
                request.wait(30.0)
        print("client functional PASS", flush=True)
    finally:
        endpoint.close(30.0)
        worker.close()


def cancel_and_timeout() -> None:
    worker = _ucx.Worker()
    try:
        request = worker.post_receive(0x1234, 64)
        try:
            request.wait(0.05)
        except RuntimeError:
            pass
        else:
            raise AssertionError("receive timeout did not fail")

        request = worker.post_receive(0x1235, 64)
        request.cancel()
        print("cancel/timeout PASS", flush=True)
    finally:
        worker.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("server", "client", "cancel"))
    parser.add_argument("address_file", type=Path)
    args = parser.parse_args()
    if args.role == "server":
        server(args.address_file)
    elif args.role == "client":
        client(args.address_file)
    elif args.role == "cancel":
        cancel_and_timeout()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
