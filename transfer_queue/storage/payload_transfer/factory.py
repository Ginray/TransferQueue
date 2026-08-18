# Copyright 2026 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Construction helper for optional SimpleStorage payload transfer."""

from transfer_queue.storage.payload_transfer.base import PayloadTransfer

_SUPPORTED_TRANSFERS = frozenset({"zmq", "ucx"})


def normalize_payload_transfer(value: object = "zmq") -> str:
    """Return a validated SimpleStorage payload transfer name."""
    normalized = str(value).strip().lower()
    if normalized not in _SUPPORTED_TRANSFERS:
        raise ValueError(f"unsupported SimpleStorage payload transfer: {normalized!r}; expected 'zmq' or 'ucx'")
    return normalized


def create_payload_transfer(value: object = "zmq", local_ip: str | None = None) -> PayloadTransfer | None:
    """Create the optional data plane; ``zmq`` keeps the existing path."""
    normalized = normalize_payload_transfer(value)
    if normalized == "zmq":
        return None

    from transfer_queue.storage.payload_transfer.ucx import UcxPayloadTransfer

    return UcxPayloadTransfer(local_ip=local_ip)
