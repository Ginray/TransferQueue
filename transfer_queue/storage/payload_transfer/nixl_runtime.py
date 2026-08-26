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

"""Small NIXL runtime used by the SimpleStorage H2H payload adapter."""

from __future__ import annotations

import ctypes
import os
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from transfer_queue.storage.payload_transfer.base import (
    PayloadDescriptor,
    PayloadTransferError,
)
from transfer_queue.storage.payload_transfer.ucx_discovery import discover_ucx_device
from transfer_queue.utils.logging_utils import get_logger
from transfer_queue.utils.serial_utils import initialize_packed_frame_table

logger = get_logger(__name__)

DEFAULT_NIXL_TRANSFER_TIMEOUT_SECONDS = 180


class NixlError(PayloadTransferError):
    """A NIXL runtime operation failed."""


@dataclass
class _RegisteredReceiveBuffer:
    buffer: bytearray
    registrations: Any

    @property
    def capacity(self) -> int:
        return len(self.buffer)


@dataclass
class _ReceiveState:
    scratch: _RegisteredReceiveBuffer
    serialized_frame_descs: bytes


@dataclass
class _FrameRegistration:
    """One source frame registration kept alive for an active transfer."""

    owner: bytearray | memoryview
    registrations: Any
    address: int
    size: int


def _buffer_address(buffer: bytearray | memoryview) -> int:
    """Return the address of a writable, contiguous host buffer."""
    if not buffer:
        raise NixlError("NIXL does not support an empty payload buffer")
    return ctypes.addressof(ctypes.c_ubyte.from_buffer(buffer))


def _configure_ucx_environment(local_ip: str | None) -> dict[str, str]:
    """Reuse UCX device discovery for NIXL's environment-based UCX backend."""
    selection = discover_ucx_device(local_ip)
    if selection.rdma_device is None:
        return {}

    config = selection.ucx_config
    for key, value in config.items():
        os.environ.setdefault(f"UCX_{key}", value)
    return config


class NixlRuntime:
    """Own one NIXL agent and serialize metadata updates safely.

    SimpleStorage's control plane already orders prepare/send/commit.  The
    runtime therefore only keeps registered receive buffers alive and waits
    for the sender-side NIXL request to finish.
    """

    def __init__(self, local_ip: str | None = None):
        ucx_config = _configure_ucx_environment(local_ip)
        try:
            from nixl import nixl_agent, nixl_agent_config
        except Exception as exc:  # pragma: no cover - optional dependency
            raise NixlError("NIXL Python bindings are unavailable; install a NIXL build with the UCX backend") from exc

        self._agent_name = self._make_agent_name()
        try:
            config = nixl_agent_config(
                enable_prog_thread=True,
                enable_listen_thread=True,
                listen_port=0,
                backends=["UCX"],
            )
            self._agent = nixl_agent(self._agent_name, config)
        except Exception as exc:  # pragma: no cover - native runtime dependent
            raise NixlError(f"failed to initialize the NIXL UCX backend: {exc}") from exc

        if "UCX" not in getattr(self._agent, "backends", {}):
            raise NixlError("NIXL UCX backend is not available")

        logger.info(
            "SimpleStorage payload transfer selected: nixl-ucx local_ip=%s device=%s gid_index=%s tls=%s",
            local_ip or "auto",
            ucx_config.get("NET_DEVICES", os.environ.get("UCX_NET_DEVICES", "auto")),
            ucx_config.get("IB_GID_INDEX", os.environ.get("UCX_IB_GID_INDEX", "auto")),
            ucx_config.get("TLS", os.environ.get("UCX_TLS", "ucx-default")),
        )

        self._receives: dict[str, _ReceiveState] = {}
        self._reusable_receive_buffer: _RegisteredReceiveBuffer | None = None
        self._remote_metadata: dict[str, bytes] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._timeout_seconds = DEFAULT_NIXL_TRANSFER_TIMEOUT_SECONDS
        self._send_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tq-nixl-send")

    @staticmethod
    def _make_agent_name() -> str:
        return f"tq-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:12]}"

    @property
    def agent_name(self) -> str:
        return self._agent_name

    def endpoint_metadata(self) -> bytes:
        with self._lock:
            self._ensure_open()
            return self._agent.get_agent_metadata()

    def prepare_receive(self, descriptor: PayloadDescriptor) -> dict[str, Any]:
        descriptor.validate()
        if not descriptor.frame_sizes or not any(descriptor.frame_sizes):
            raise NixlError("NIXL direct-frame receive requires a non-empty frame")
        with self._lock:
            self._ensure_open()
            if descriptor.transfer_id in self._receives:
                raise NixlError(f"duplicate NIXL receive: {descriptor.transfer_id}")
            scratch = self._acquire_receive_buffer(descriptor.payload_bytes)
            try:
                initialize_packed_frame_table(scratch.buffer, descriptor.frame_sizes)
                address = _buffer_address(scratch.buffer)
                payload_offset = 8 + 16 * len(descriptor.frame_sizes)
                regions = []
                for size in descriptor.frame_sizes:
                    if size:
                        regions.append((address + payload_offset, size, 0))
                    payload_offset += size
                frame_descs = self._agent.get_serialized_descs(self._agent.get_xfer_descs(regions, mem_type="DRAM"))
                state = _ReceiveState(scratch, frame_descs)
                metadata = self._agent.get_agent_metadata()
                self._receives[descriptor.transfer_id] = state
            except Exception as exc:
                self._return_receive_buffer(scratch)
                raise NixlError(f"failed to register NIXL receive buffer: {exc}") from exc
        result = {
            "agent_name": self._agent_name,
            "agent_metadata": metadata,
            "frame_remote_descs": state.serialized_frame_descs,
            "payload_bytes": descriptor.payload_bytes,
        }
        return result

    def send(
        self,
        endpoint: dict[str, Any],
        token: dict[str, Any],
        descriptor: PayloadDescriptor,
        frames: tuple[bytes | bytearray | memoryview, ...],
    ) -> Future[None]:
        """Run the NIXL transfer on the dedicated send thread."""
        descriptor.validate()
        if not descriptor.frame_sizes or not any(descriptor.frame_sizes):
            raise NixlError("NIXL direct-frame send requires a non-empty frame")
        if tuple(memoryview(frame).nbytes for frame in frames) != descriptor.frame_sizes:
            raise NixlError(f"frame lengths do not match descriptor for {descriptor.transfer_id}")
        try:
            remote_name, metadata = self._validate_send_metadata(endpoint, token, descriptor)
        except Exception as exc:
            future: Future[None] = Future()
            future.set_exception(exc)
            return future
        with self._lock:
            self._ensure_open()
            return self._send_executor.submit(
                self._send_scatter,
                remote_name,
                metadata,
                tuple(frames),
                token["frame_remote_descs"],
            )

    def _acquire_receive_buffer(self, required_capacity: int) -> _RegisteredReceiveBuffer:
        reusable = self._reusable_receive_buffer
        if reusable is not None and reusable.capacity >= required_capacity:
            self._reusable_receive_buffer = None
            return reusable

        buffer = bytearray(required_capacity)
        address = _buffer_address(buffer)
        registrations = self._agent.register_memory(
            [(address, required_capacity, 0, "")], mem_type="DRAM", backends=["UCX"]
        )
        if registrations is None:
            raise NixlError("failed to register NIXL receive scratch buffer")
        allocated = _RegisteredReceiveBuffer(buffer, registrations)
        if reusable is not None:
            self._reusable_receive_buffer = None
            self._deregister_registration(reusable.registrations)
        return allocated

    def _return_receive_buffer(self, scratch: _RegisteredReceiveBuffer) -> None:
        reusable = self._reusable_receive_buffer
        if reusable is None:
            self._reusable_receive_buffer = scratch
        elif scratch.capacity > reusable.capacity:
            self._deregister_registration(reusable.registrations)
            self._reusable_receive_buffer = scratch
        else:
            self._deregister_registration(scratch.registrations)

    def _acquire_frame_registration(self, frame: bytes | bytearray | memoryview) -> _FrameRegistration:
        view = memoryview(frame)
        if view.readonly or not view.c_contiguous:
            owner: bytearray | memoryview = bytearray(view)
        else:
            owner = view.cast("B")
        address = _buffer_address(owner)
        registrations = self._agent.register_memory(
            [(address, memoryview(owner).nbytes, 0, "")], mem_type="DRAM", backends=["UCX"]
        )
        if registrations is None:
            raise NixlError("failed to register NIXL source frame")
        return _FrameRegistration(owner, registrations, address, memoryview(owner).nbytes)

    def _send_scatter(
        self,
        remote_name: str,
        metadata: bytes,
        frames: tuple[bytes | bytearray | memoryview, ...],
        serialized_remote_descs: bytes,
    ) -> None:
        """Send frames directly to matching registered remote regions."""
        frame_registrations: list[_FrameRegistration] = []
        handle = None
        try:
            with self._lock:
                self._ensure_open()
                previous = self._remote_metadata.get(remote_name)
                if previous != metadata:
                    if previous is not None:
                        self._agent.remove_remote_agent(remote_name)
                    loaded_name = self._agent.add_remote_agent(metadata)
                    if isinstance(loaded_name, bytes):
                        loaded_name = loaded_name.decode()
                    if loaded_name != remote_name:
                        raise NixlError(
                            f"NIXL remote agent name mismatch: expected {remote_name!r}, got {loaded_name!r}"
                        )
                    self._remote_metadata[remote_name] = metadata

                for frame in frames:
                    if memoryview(frame).nbytes:
                        frame_registrations.append(self._acquire_frame_registration(frame))
                local_descs = self._agent.get_xfer_descs(
                    [(registration.address, registration.size, 0) for registration in frame_registrations],
                    mem_type="DRAM",
                )
                remote_descs = self._agent.deserialize_descs(serialized_remote_descs)
                handle = self._agent.initialize_xfer("WRITE", local_descs, remote_descs, remote_name, backends=["UCX"])
                status = self._agent.transfer(handle)

            deadline = time.monotonic() + self._timeout_seconds
            while status == "PROC":
                if time.monotonic() >= deadline:
                    raise NixlError(f"NIXL WRITE timed out after {self._timeout_seconds:g}s")
                with self._lock:
                    self._ensure_open()
                    status = self._agent.check_xfer_state(handle)
                if status == "PROC":
                    time.sleep(0.0005)
            if status != "DONE":
                raise NixlError(f"NIXL WRITE failed with status {status!r}")
        except NixlError:
            raise
        except Exception as exc:
            raise NixlError(f"NIXL H2H scatter WRITE failed: {exc}") from exc
        finally:
            with self._lock:
                if handle is not None:
                    try:
                        self._agent.release_xfer_handle(handle)
                    except Exception:
                        pass
                for registration in frame_registrations:
                    self._deregister_registration(registration.registrations)

    @staticmethod
    def _validate_send_metadata(
        endpoint: dict[str, Any],
        token: dict[str, Any],
        descriptor: PayloadDescriptor,
    ) -> tuple[str, bytes]:
        remote_name = str(token.get("agent_name") or endpoint.get("agent_name") or "")
        metadata = token.get("agent_metadata") or endpoint.get("agent_metadata")
        if not remote_name or not isinstance(metadata, bytes):
            raise NixlError("NIXL endpoint is missing remote agent metadata")
        if int(token.get("payload_bytes", -1)) != descriptor.payload_bytes:
            raise NixlError("NIXL receive token length does not match descriptor")
        if not isinstance(token.get("frame_remote_descs"), bytes):
            raise NixlError("NIXL receive token is missing frame descriptors")
        return remote_name, metadata

    def receive(self, descriptor: PayloadDescriptor) -> Future[memoryview]:
        descriptor.validate()
        future: Future[memoryview] = Future()
        with self._lock:
            self._ensure_open()
            state = self._receives.pop(descriptor.transfer_id, None)
            if state is None:
                future.set_exception(NixlError(f"no prepared NIXL receive for {descriptor.transfer_id}"))
                return future
            try:
                # Decoded tensors may outlive release_receive(). Detach their
                # backing storage from the registered scratch before reuse.
                payload = bytearray(state.scratch.buffer[: descriptor.payload_bytes])
                future.set_result(memoryview(payload))
            except Exception as exc:
                future.set_exception(NixlError(f"failed to copy NIXL receive payload: {exc}"))
            finally:
                self._return_receive_buffer(state.scratch)
        return future

    def cancel_receive(self, transfer_id: str) -> None:
        with self._lock:
            state = self._receives.pop(transfer_id, None)
            if state is None:
                return
            try:
                self._deregister_registration(state.scratch.registrations)
            except Exception:
                pass

    def release_receive(self, transfer_id: str) -> None:
        """Compatibility no-op; receive() already detached and returned its scratch."""

    @property
    def pending_receive_count(self) -> int:
        with self._lock:
            return len(self._receives)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._send_executor.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            for state in self._receives.values():
                self._deregister_registration(state.scratch.registrations)
            self._receives.clear()
            if self._reusable_receive_buffer is not None:
                self._deregister_registration(self._reusable_receive_buffer.registrations)
                self._reusable_receive_buffer = None
        self._agent = None

    def _ensure_open(self) -> None:
        if self._closed:
            raise NixlError("NIXL runtime is closed")

    def _deregister_registration(self, registrations: Any) -> None:
        try:
            self._agent.deregister_memory(registrations, backends=["UCX"])
        except Exception:
            pass


def create_nixl_runtime(local_ip: str | None = None) -> NixlRuntime:
    return NixlRuntime(local_ip=local_ip)
