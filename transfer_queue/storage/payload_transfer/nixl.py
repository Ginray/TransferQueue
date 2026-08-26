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

"""NIXL H2H implementation of the SimpleStorage payload transfer contract."""

from __future__ import annotations

from concurrent.futures import Future

from transfer_queue.storage.payload_transfer.base import (
    PayloadDescriptor,
    PayloadTransfer,
    PayloadTransferError,
    ReceiveToken,
    TransferEndpoint,
)
from transfer_queue.storage.payload_transfer.nixl_runtime import (
    NixlError,
    create_nixl_runtime,
)


class NixlPayloadTransfer(PayloadTransfer):
    """Adapt NIXL DRAM ``WRITE`` operations to the payload transfer contract."""

    transport = "nixl-ucx"

    def __init__(self, local_ip: str | None = None):
        try:
            self._runtime = create_nixl_runtime(local_ip)
        except NixlError:
            raise
        except Exception as exc:
            raise NixlError(f"failed to create NIXL runtime: {exc}") from exc

    def endpoint(self) -> TransferEndpoint:
        return TransferEndpoint(
            transport=self.transport,
            data={
                "agent_name": self._runtime.agent_name,
                "agent_metadata": self._runtime.endpoint_metadata(),
            },
        )

    def prepare_receive(self, descriptor: PayloadDescriptor) -> ReceiveToken:
        return ReceiveToken(data=self._runtime.prepare_receive(descriptor))

    def send(
        self,
        endpoint: TransferEndpoint,
        token: ReceiveToken,
        descriptor: PayloadDescriptor,
        frames: tuple[bytes | bytearray | memoryview, ...] | list[bytes | bytearray | memoryview],
    ) -> Future[None]:
        """Send encoded frames directly through NIXL scatter-gather."""
        self._validate_metadata(endpoint, token, descriptor)
        return self._runtime.send(endpoint.data, token.data, descriptor, tuple(frames))

    def receive(self, descriptor: PayloadDescriptor) -> Future[memoryview]:
        return self._runtime.receive(descriptor)

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
    def _validate_metadata(
        endpoint: TransferEndpoint,
        token: ReceiveToken,
        descriptor: PayloadDescriptor,
    ) -> None:
        descriptor.validate()
        if endpoint.transport != "nixl-ucx":
            raise PayloadTransferError(f"NIXL cannot use endpoint for {endpoint.transport!r}")
        if token.data.get("agent_name") != endpoint.data.get("agent_name"):
            raise PayloadTransferError("NIXL endpoint and receive token agent names differ")
        if not isinstance(token.data.get("frame_remote_descs"), bytes):
            raise PayloadTransferError("NIXL receive token is missing frame descriptors")
        if not isinstance(token.data.get("agent_metadata"), bytes):
            raise PayloadTransferError("NIXL receive token is missing agent metadata")
