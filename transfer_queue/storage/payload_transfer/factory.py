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

"""Construction helper for optional SimpleStorage payload transfer."""

from collections.abc import Mapping

from transfer_queue.storage.payload_transfer.base import PayloadTransfer

_SUPPORTED_TRANSFERS = frozenset({"nixl-ucx", "zmq"})


def parse_payload_transfer_config(value: object | None = None) -> tuple[str, dict[str, object]]:
    """Return the backend name and options from the payload transfer block."""
    config = {"backend": "zmq"} if value is None else value
    if not isinstance(config, Mapping):
        raise TypeError("SimpleStorage.payload_transfer must be a mapping")

    backend = str(config["backend"]).strip().lower()
    if backend not in _SUPPORTED_TRANSFERS:
        raise ValueError(f"unsupported SimpleStorage payload transfer: {backend!r}; expected 'zmq' or 'nixl-ucx'")
    ucx_env_vars = config.get("ucx_env_vars", {})
    if not isinstance(ucx_env_vars, Mapping):
        raise TypeError("SimpleStorage.payload_transfer.ucx_env_vars must be a mapping")
    return backend, dict(ucx_env_vars)


def create_payload_transfer(value: object | None = None) -> PayloadTransfer | None:
    """Create the optional data plane from a payload transfer config block."""
    backend, ucx_env_vars = parse_payload_transfer_config(value)
    if backend == "zmq":
        return None
    if backend == "nixl-ucx":
        from transfer_queue.storage.payload_transfer.nixl import NixlPayloadTransfer

        return NixlPayloadTransfer(ucx_env_vars=ucx_env_vars)

    raise RuntimeError(f"unhandled SimpleStorage payload transfer: {backend!r}")
