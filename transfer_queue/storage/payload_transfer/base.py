# Copyright 2025 Huawei Technologies Co., Ltd. All Rights Reserved.
# Copyright 2025 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Optional payload transfer contract used by SimpleStorage."""

from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any


class PayloadTransferError(RuntimeError):
    """A payload transfer could not be completed safely."""


@dataclass(frozen=True)
class PayloadDescriptor:
    """Description of one encoded payload."""

    transfer_id: str
    payload_bytes: int
    # The packed size remains part of the control-plane contract; transports
    # may use the frame layout for scatter/gather transfer.
    frame_sizes: tuple[int, ...]

    @staticmethod
    def validate_transfer_id(transfer_id: str) -> None:
        """Validate the identifier used to correlate one transfer."""
        if not transfer_id or len(transfer_id) > 128:
            raise PayloadTransferError("transfer_id must contain 1 to 128 characters")

    def validate(self) -> None:
        """Validate payload length and packed frame metadata."""
        self.validate_transfer_id(self.transfer_id)
        if self.payload_bytes < 0:
            raise PayloadTransferError(f"negative payload length for {self.transfer_id}")
        if any(size < 0 for size in self.frame_sizes):
            raise PayloadTransferError(f"negative frame length for {self.transfer_id}")
        packed_size = 4 + 8 * len(self.frame_sizes) + sum(self.frame_sizes)
        if packed_size != self.payload_bytes:
            raise PayloadTransferError(
                f"packed payload length mismatch for {self.transfer_id}: "
                f"expected {packed_size}, got {self.payload_bytes}"
            )

    def to_dict(self) -> dict[str, int | str | list[int]]:
        """Serialize the descriptor for the ZMQ control plane."""
        return {
            "transfer_id": self.transfer_id,
            "payload_bytes": self.payload_bytes,
            "frame_sizes": list(self.frame_sizes),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PayloadDescriptor:
        """Deserialize and validate a descriptor received over the wire."""
        descriptor = cls(
            transfer_id=str(value["transfer_id"]),
            payload_bytes=int(value["payload_bytes"]),
            frame_sizes=tuple(int(size) for size in value["frame_sizes"]),
        )
        descriptor.validate()
        return descriptor


@dataclass
class TransferEndpoint:
    """Bootstrap metadata for one payload transfer endpoint."""

    transport: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize endpoint metadata for bootstrap or control messages."""
        return {"transport": self.transport, "data": self.data}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TransferEndpoint:
        """Deserialize endpoint metadata received from a peer."""
        return cls(transport=str(value["transport"]), data=dict(value["data"]))


@dataclass
class ReceiveToken:
    """Transport-owned metadata returned after preparing a receive."""

    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Serialize transport-owned receive metadata for the control plane."""
        return {"data": self.data}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ReceiveToken:
        """Deserialize transport-owned receive metadata from a peer."""
        return cls(data=dict(value["data"]))


class PayloadTransfer(ABC):
    """Optional payload data plane; ZMQ remains the control path."""

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
        frames: list[bytes | bytearray | memoryview] | tuple[bytes | bytearray | memoryview, ...],
    ) -> Future[None]:
        """Send encoded frames directly using the provider data path."""

    @abstractmethod
    def receive(self, descriptor: PayloadDescriptor) -> Future[memoryview]:
        """Return a future for a prepared receive."""

    @abstractmethod
    def cancel_receive(self, transfer_id: str) -> None:
        """Start best-effort cancellation of a prepared receive."""

    @abstractmethod
    def close(self) -> None:
        """Release all transport resources."""
