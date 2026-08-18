#!/usr/bin/env python3
"""Measure a ZMQ multipart payload with a receive-side acknowledgement."""

from __future__ import annotations

import argparse
import statistics
import time

import torch
import zmq

from transfer_queue.utils.serial_utils import encode


def make_frames(elements: int) -> list:
    return encode({"tensor": [torch.arange(elements, dtype=torch.int64)]})


def server(bind_address: str, elements: int, repetitions: int) -> None:
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(bind_address)
    try:
        for _ in range(repetitions):
            frames = socket.recv_multipart(copy=False)
            socket.send(b"ok")
            if not frames or sum(memoryview(frame).nbytes for frame in frames) == 0:
                raise AssertionError("empty multipart payload")
        print(f"zmq server PASS elements={elements} repetitions={repetitions}", flush=True)
    finally:
        socket.close(0)
        context.term()


def client(address: str, elements: int, repetitions: int) -> None:
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(address)
    frames = make_frames(elements)
    elapsed = []
    try:
        for _ in range(repetitions):
            start = time.perf_counter()
            socket.send_multipart(frames, copy=False)
            if socket.recv() != b"ok":
                raise AssertionError("invalid server acknowledgement")
            elapsed.append(time.perf_counter() - start)
        measured = elapsed[1:] if len(elapsed) > 1 else elapsed
        median_seconds = statistics.median(measured)
        payload_bytes = sum(memoryview(frame).nbytes for frame in frames)
        print(
            f"zmq client PASS elements={elements} repetitions={repetitions} "
            f"median_seconds={median_seconds:.6f} "
            f"throughput_mib_s={payload_bytes / median_seconds / 2**20:.2f}",
            flush=True,
        )
    finally:
        socket.close(0)
        context.term()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("server", "client"))
    parser.add_argument("address")
    parser.add_argument("--elements", type=int, default=1_000_000)
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    if args.role == "server":
        server(args.address, args.elements, args.repetitions)
    else:
        client(args.address, args.elements, args.repetitions)


if __name__ == "__main__":
    main()
