# Copyright 2026 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Optional payload transfer contract used by SimpleStorage."""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

DEFAULT_INLINE_PAYLOAD_BYTES = 128 * 1024


class PayloadTransferError(RuntimeError):
    """A payload transfer could not be completed safely."""


@dataclass(frozen=True)
class PayloadDescriptor:
    """Description of one encoded payload."""

    transfer_id: str
    payload_bytes: int

    @staticmethod
    def validate_transfer_id(transfer_id: str) -> None:
        if not transfer_id or len(transfer_id) > 128:
            raise PayloadTransferError("transfer_id must contain 1 to 128 characters")

    def validate(self) -> None:
        self.validate_transfer_id(self.transfer_id)
        if self.payload_bytes < 0:
            raise PayloadTransferError(f"negative payload length for {self.transfer_id}")

    def to_dict(self) -> dict[str, int | str]:
        return {
            "transfer_id": self.transfer_id,
            "payload_bytes": self.payload_bytes,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PayloadDescriptor:
        descriptor = cls(
            transfer_id=str(value["transfer_id"]),
            payload_bytes=int(value["payload_bytes"]),
        )
        descriptor.validate()
        return descriptor


@dataclass
class TransferEndpoint:
    """Bootstrap metadata for one payload transfer endpoint."""

    transport: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"transport": self.transport, "data": self.data}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TransferEndpoint:
        return cls(transport=str(value["transport"]), data=dict(value["data"]))


@dataclass
class ReceiveToken:
    """Transport-owned metadata returned after preparing a receive."""

    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"data": self.data}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ReceiveToken:
        return cls(data=dict(value["data"]))


class PayloadTransfer(ABC):
    """Optional payload data plane; ZMQ remains the control and inline path."""

    transport: str

    @abstractmethod
    def endpoint(self) -> TransferEndpoint:
        """Return bootstrap-safe endpoint metadata."""

    @abstractmethod
    def prepare_receive(self, descriptor: PayloadDescriptor) -> ReceiveToken:
        """Prepare a receive and return the token required by the sender."""

    @abstractmethod
    def send(
        self,
        endpoint: TransferEndpoint,
        token: ReceiveToken,
        descriptor: PayloadDescriptor,
        payload: bytes | bytearray | memoryview,
    ) -> Future[None]:
        """Start sending a payload and return its completion future."""

    @abstractmethod
    def receive(self, descriptor: PayloadDescriptor) -> Future[memoryview]:
        """Return a future for a prepared receive."""

    @abstractmethod
    def cancel_receive(self, transfer_id: str) -> None:
        """Start best-effort cancellation of a prepared receive."""

    @property
    @abstractmethod
    def pending_receive_count(self) -> int:
        """Return the number of prepared receives not yet completed."""

    @abstractmethod
    def close(self) -> None:
        """Release all transport resources."""
