#!/usr/bin/env python3
"""Verify malformed short UCX addresses fail in Python, not in native UCX."""

from __future__ import annotations

from transfer_queue.storage.data_plane import (
    DataPlaneError,
    PayloadDescriptor,
    UcxDataPlane,
    address_digest,
    transfer_tag,
)


def main() -> None:
    data_plane = UcxDataPlane(timeout_seconds=2)
    try:
        transfer_id = "malformed-address-guard"
        descriptor = PayloadDescriptor(
            transfer_id=transfer_id,
            tag=transfer_tag(transfer_id),
            payload_bytes=1,
            frame_count=1,
        )
        try:
            data_plane.send(b"malformed", descriptor, b"x")
        except DataPlaneError as exc:
            print(f"malformed address guard PASS: {exc}", flush=True)
        else:
            raise AssertionError("malformed address unexpectedly reached UCX")

        valid_address = data_plane.address
        corrupt_address = bytearray(valid_address)
        corrupt_address[0] ^= 0xFF
        try:
            data_plane.send(
                bytes(corrupt_address),
                descriptor,
                b"x",
                peer_address_digest=address_digest(valid_address),
            )
        except DataPlaneError as exc:
            print(f"same-length digest guard PASS: {exc}", flush=True)
        else:
            raise AssertionError("same-length corrupt address bypassed digest guard")
    finally:
        data_plane.close()


if __name__ == "__main__":
    main()
