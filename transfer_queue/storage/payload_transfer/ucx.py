# Copyright 2026 Huawei Technologies Co., Ltd. All Rights Reserved.
# Copyright 2026 The TransferQueue Team
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

"""UCX implementation of the SimpleStorage payload transfer contract."""

from __future__ import annotations

from concurrent.futures import Future

from transfer_queue.storage.payload_transfer.base import (
    PayloadDescriptor,
    PayloadTransfer,
    PayloadTransferError,
    ReceiveToken,
    TransferEndpoint,
)
from transfer_queue.storage.payload_transfer.ucx_runtime import (
    UcxTransfer,
    address_digest,
    create_ucx_runtime,
    transfer_tag,
)


class UcxPayloadTransfer(PayloadTransfer):
    """Adapt UCX Tagged operations to the payload transfer contract."""

    transport = "ucx"

    def __init__(self, local_ip: str | None = None):
        self._runtime = create_ucx_runtime(local_ip)

    def endpoint(self) -> TransferEndpoint:
        address = self._runtime.address
        return TransferEndpoint(
            transport=self.transport,
            data={"address": address, "address_digest": address_digest(address)},
        )

    def prepare_receive(self, descriptor: PayloadDescriptor) -> ReceiveToken:
        self._runtime.prepare_receive(self._ucx_transfer(descriptor))
        return ReceiveToken(data={})

    def send(
        self,
        endpoint: TransferEndpoint,
        token: ReceiveToken,
        descriptor: PayloadDescriptor,
        frames: tuple[bytes | bytearray | memoryview, ...] | list[bytes | bytearray | memoryview],
    ) -> Future[None]:
        """Send the encoder's frames directly as tagged UCX messages."""
        self._validate_peer_metadata(endpoint, token, descriptor)
        if not descriptor.frame_sizes:
            raise PayloadTransferError("UCX direct frame send requires frame sizes")
        transfer = self._ucx_transfer(descriptor)
        return self._runtime.send(
            endpoint.data["address"],
            transfer,
            tuple(frames),
            endpoint.data.get("address_digest"),
        )

    def receive(self, descriptor: PayloadDescriptor) -> Future[memoryview]:
        return self._runtime.finish_receive_future(self._ucx_transfer(descriptor))

    def cancel_receive(self, transfer_id: str) -> None:
        self._runtime.cancel_receive(transfer_id)

    def release_receive(self, transfer_id: str) -> None:
        self._runtime.release_receive(transfer_id)

    @property
    def pending_receive_count(self) -> int:
        return self._runtime.pending_receive_count

    def close(self) -> None:
        self._runtime.close()

    @staticmethod
    def _ucx_transfer(descriptor: PayloadDescriptor) -> UcxTransfer:
        descriptor.validate()
        return UcxTransfer(
            transfer_id=descriptor.transfer_id,
            tag=transfer_tag(descriptor.transfer_id),
            payload_bytes=descriptor.payload_bytes,
            frame_sizes=descriptor.frame_sizes,
        )

    def _validate_peer_metadata(
        self,
        endpoint: TransferEndpoint,
        token: ReceiveToken,
        descriptor: PayloadDescriptor,
    ) -> None:
        descriptor.validate()
        if endpoint.transport != self.transport:
            raise PayloadTransferError(f"UCX cannot use endpoint for {endpoint.transport!r}")
        if token.data:
            raise PayloadTransferError("UCX receive token must be empty")
        if not isinstance(endpoint.data.get("address"), bytes):
            raise PayloadTransferError("UCX endpoint address must be bytes")
