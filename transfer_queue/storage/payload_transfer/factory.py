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

"""Construction helper for optional SimpleStorage payload transfer."""

from transfer_queue.storage.payload_transfer.base import PayloadTransfer

_SUPPORTED_TRANSFERS = frozenset({"nixl-ucx", "ucx", "zmq"})


def normalize_payload_transfer(value: object = "zmq") -> str:
    """Return a validated SimpleStorage payload transfer name."""
    normalized = str(value).strip().lower()
    if normalized not in _SUPPORTED_TRANSFERS:
        raise ValueError(
            f"unsupported SimpleStorage payload transfer: {normalized!r}; "
            "expected 'zmq', 'ucx' or 'nixl-ucx'"
        )
    return normalized


def create_payload_transfer(value: object = "zmq", local_ip: str | None = None) -> PayloadTransfer | None:
    """Create the optional data plane; ``zmq`` keeps the existing path."""
    normalized = normalize_payload_transfer(value)
    if normalized == "zmq":
        return None
    if normalized == "nixl-ucx":
        from transfer_queue.storage.payload_transfer.nixl import NixlPayloadTransfer

        return NixlPayloadTransfer(local_ip=local_ip)
    if normalized == "ucx":
        from transfer_queue.storage.payload_transfer.ucx import UcxPayloadTransfer

        return UcxPayloadTransfer(local_ip=local_ip)

    raise RuntimeError(f"unhandled SimpleStorage payload transfer: {normalized!r}")
