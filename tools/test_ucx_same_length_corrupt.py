#!/usr/bin/env python3
"""Probe the remaining same-length-corrupt UCX address boundary."""

from __future__ import annotations

from transfer_queue.storage.payload_transfer.ucx_runtime import UcxError, UcxRuntime, UcxTransfer, transfer_tag


def main() -> None:
    runtime = UcxRuntime(timeout_seconds=2)
    try:
        address = bytearray(runtime.address)
        if len(address) < 16:
            raise AssertionError(f"unexpected worker address length: {len(address)}")
        # The first byte is the UCP address version in the current UCX wire
        # format.  Use the invalid value from the previously observed abort.
        address[0] = 9
        descriptor = UcxTransfer(
            transfer_id="same-length-corrupt-address",
            tag=transfer_tag("same-length-corrupt-address"),
            payload_bytes=1,
        )
        try:
            runtime.send(bytes(address), descriptor, b"x")
        except UcxError as exc:
            print(f"same-length address guard PASS: {exc}", flush=True)
        else:
            raise AssertionError("corrupt address unexpectedly connected")
    finally:
        runtime.close()


if __name__ == "__main__":
    main()
