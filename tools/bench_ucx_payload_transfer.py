#!/usr/bin/env python3
"""Measure TQ UCX Host payload transfer without Ray or SimpleStorage."""

from __future__ import annotations

import argparse
import base64
import hashlib
import statistics
import time
from pathlib import Path

from transfer_queue.storage.payload_transfer.ucx_runtime import UcxRuntime, UcxTransfer, address_digest, transfer_tag


def make_payload(size: int, iteration: int) -> bytearray:
    seed = hashlib.sha256(f"tq-bench:{size}:{iteration}".encode()).digest()
    return bytearray((seed * ((size // len(seed)) + 1))[:size])


def descriptor(size: int, iteration: int) -> UcxTransfer:
    transfer_id = f"tq-bench-{size}-{iteration}"
    return UcxTransfer(transfer_id, transfer_tag(transfer_id), size)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("server", "client"))
    parser.add_argument("address_file", type=Path)
    parser.add_argument("--size", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()

    dp = UcxRuntime(180)
    try:
        if args.role == "server":
            # Post the first receive before publishing the address.  Otherwise
            # the client can start the first rendezvous before the server has
            # installed its matching receive, which is not representative of
            # SimpleStorage's PREPARE handshake.
            first = descriptor(args.size, 0)
            dp.prepare_receive(first)
            args.address_file.write_text(base64.b64encode(dp.address).decode())
            for iteration in range(args.repetitions):
                item = descriptor(args.size, iteration)
                if iteration > 0:
                    dp.prepare_receive(item)
                actual = dp.finish_receive(item)
                if len(actual) != args.size:
                    raise AssertionError(f"payload length mismatch at iteration {iteration}")
            print(f"bench server PASS size={args.size} repetitions={args.repetitions}", flush=True)
            return

        deadline = time.monotonic() + 60
        while not args.address_file.exists():
            if time.monotonic() > deadline:
                raise TimeoutError("server address file was not created")
            time.sleep(0.1)
        peer = base64.b64decode(args.address_file.read_text())
        peer_digest = address_digest(peer)
        dp.warmup(peer, peer_digest)
        elapsed = []
        for iteration in range(args.repetitions):
            item = descriptor(args.size, iteration)
            start = time.perf_counter()
            dp.send(peer, item, make_payload(args.size, iteration), peer_address_digest=peer_digest)
            elapsed.append(time.perf_counter() - start)
        measured = elapsed[1:] if len(elapsed) > 1 else elapsed
        median_seconds = statistics.median(measured)
        mib_s = args.size / median_seconds / 2**20
        print(
            f"bench client PASS size={args.size} repetitions={args.repetitions} "
            f"median_seconds={median_seconds:.6f} throughput_mib_s={mib_s:.2f}",
            flush=True,
        )
    finally:
        dp.close()


if __name__ == "__main__":
    main()
