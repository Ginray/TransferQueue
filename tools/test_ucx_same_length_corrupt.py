#!/usr/bin/env python3
"""Probe the remaining same-length-corrupt UCX address boundary."""

from __future__ import annotations

from transfer_queue.storage.data_plane import DataPlaneError, PayloadDescriptor, UcxDataPlane, transfer_tag


def main() -> None:
    data_plane = UcxDataPlane(timeout_seconds=2)
    try:
        address = bytearray(data_plane.address)
        if len(address) < 16:
            raise AssertionError(f"unexpected worker address length: {len(address)}")
        # The first byte is the UCP address version in the current UCX wire
        # format.  Use the invalid value from the previously observed abort.
        address[0] = 9
        descriptor = PayloadDescriptor(
            transfer_id="same-length-corrupt-address",
            tag=transfer_tag("same-length-corrupt-address"),
            payload_bytes=1,
            frame_count=1,
        )
        try:
            data_plane.send(bytes(address), descriptor, b"x")
        except DataPlaneError as exc:
            print(f"same-length address guard PASS: {exc}", flush=True)
        else:
            raise AssertionError("corrupt address unexpectedly connected")
    finally:
        data_plane.close()


if __name__ == "__main__":
    main()
