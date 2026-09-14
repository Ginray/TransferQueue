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

"""Small NIXL runtime used by the SimpleStorage H2H payload adapter."""

from __future__ import annotations

import ctypes
import os
import socket
import threading
import time
import weakref
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

import numpy as np

from transfer_queue.storage.payload_transfer.base import PayloadTransferError
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
    address: int
    lease_finalizer: weakref.finalize | None = None

    @property
    def capacity(self) -> int:
        return len(self.buffer)


@dataclass
class _ReceiveState:
    scratch: _RegisteredReceiveBuffer
    serialized_frame_descs: bytes


@dataclass
class _FrameSource:
    """One source frame kept alive for an active transfer."""

    owner: bytearray | memoryview | np.ndarray
    registration: Any | None
    address: int
    size: int


def _buffer_address(buffer: bytearray | memoryview) -> int:
    """Return the address of a writable, contiguous host buffer."""
    if not buffer:
        raise NixlError("NIXL does not support an empty payload buffer")
    return ctypes.addressof(ctypes.c_ubyte.from_buffer(buffer))


def _configure_ucx_environment(ucx_env_vars: dict[str, object] | None) -> dict[str, str]:
    """Apply YAML UCX settings before the NIXL agent reads its environment."""
    config = {str(key): str(value) for key, value in (ucx_env_vars or {}).items()}
    os.environ.update(config)
    return config


def _warn_if_tcp_fallback_possible() -> None:
    """Warn when the configured UCX transports allow a TCP payload fallback."""
    ucx_tls = os.environ.get("UCX_TLS")
    if not ucx_tls:
        return

    transports = {item.strip() for item in ucx_tls.split(",") if item.strip()}
    if "tcp" not in transports and "all" not in transports:
        return

    logger.warning("NIXL-UCX may fall back to TCP; check UCX logs for the actual transport (UCX_TLS=%s)", ucx_tls)


class NixlRuntime:
    """Own one NIXL agent and serialize metadata updates safely.

    SimpleStorage's control plane already orders prepare/send/commit.  The
    runtime therefore only keeps registered receive buffers alive and waits
    for the sender-side NIXL request to finish.
    """

    def __init__(
        self,
        ucx_env_vars: dict[str, object] | None = None,
        timing_callback: Callable[[str, float], None] | None = None,
        max_cached_receive_buffers: int = 1,
    ):
        _configure_ucx_environment(ucx_env_vars)
        self._timing_callback = timing_callback
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
        self._endpoint_metadata = self._agent.get_agent_metadata()

        _warn_if_tcp_fallback_possible()
        logger.info(
            "SimpleStorage payload transfer selected: nixl-ucx device=%s gid_index=%s tls=%s",
            os.environ.get("UCX_NET_DEVICES", "ucx-auto"),
            os.environ.get("UCX_IB_GID_INDEX", "ucx-auto"),
            os.environ.get("UCX_TLS", "ucx-auto"),
        )

        self._receives: dict[str, _ReceiveState] = {}
        self._receive_pool: list[_RegisteredReceiveBuffer] = []
        self._max_cached_receive_buffers = max(1, max_cached_receive_buffers)
        self._leased_receive_buffers: dict[int, _RegisteredReceiveBuffer] = {}
        self._failed_deregistrations: list[tuple[Any, Any]] = []
        self._remote_metadata: dict[str, bytes] = {}
        self._lock = threading.RLock()
        self._closed = False
        self._timeout_seconds = DEFAULT_NIXL_TRANSFER_TIMEOUT_SECONDS
        self._send_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tq-nixl-send")

    def _record_timing(self, name: str, start: float) -> None:
        if self._timing_callback is not None:
            self._timing_callback(name, time.perf_counter() - start)

    @staticmethod
    def _make_agent_name() -> str:
        return f"tq-{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:12]}"

    @property
    def agent_name(self) -> str:
        """Return the stable name advertised by this runtime instance."""
        return self._agent_name

    def endpoint_metadata(self) -> bytes:
        """Return stable connection metadata; receive MRs use partial metadata."""
        with self._lock:
            self._ensure_open()
            return self._endpoint_metadata

    def prepare_receive(self, descriptor: Any) -> dict[str, Any]:
        """Allocate or reuse registered storage for a scatter receive."""
        descriptor.validate()
        if not descriptor.frame_sizes or not any(descriptor.frame_sizes):
            raise NixlError("NIXL direct-frame receive requires a non-empty frame")
        with self._lock:
            self._ensure_open()
            if descriptor.transfer_id in self._receives:
                raise NixlError(f"duplicate NIXL receive: {descriptor.transfer_id}")
            receive_start = time.perf_counter()
            scratch = self._acquire_receive_buffer(descriptor.payload_bytes)
            self._record_timing("receive_buffer_prepare", receive_start)
            try:
                descriptor_start = time.perf_counter()
                initialize_packed_frame_table(scratch.buffer, descriptor.frame_sizes)
                address = scratch.address
                payload_offset = 4 + 8 * len(descriptor.frame_sizes)
                regions = []
                for size in descriptor.frame_sizes:
                    if size:
                        regions.append((address + payload_offset, size, 0))
                    payload_offset += size
                frame_descs = self._agent.get_serialized_descs(self._agent.get_xfer_descs(regions, mem_type="DRAM"))
                self._record_timing("receive_descriptor_prepare", descriptor_start)
                state = _ReceiveState(scratch, frame_descs)
                metadata = self._agent.get_partial_agent_metadata(
                    scratch.registrations,
                    inc_conn_info=True,
                    backends=["UCX"],
                )
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
        descriptor: Any,
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
        reusable = [
            (scratch.capacity, index)
            for index, scratch in enumerate(self._receive_pool)
            if scratch.capacity >= required_capacity
        ]
        if reusable:
            _, index = min(reusable)
            return self._receive_pool.pop(index)

        allocation_start = time.perf_counter()
        buffer = bytearray(required_capacity)
        self._record_timing("receive_buffer_allocation", allocation_start)
        address = _buffer_address(buffer)
        registration_start = time.perf_counter()
        registrations = self._agent.register_memory(
            [(address, required_capacity, 0, "")], mem_type="DRAM", backends=["UCX"]
        )
        self._record_timing("receive_buffer_registration", registration_start)
        if registrations is None:
            raise NixlError("failed to register NIXL receive scratch buffer")
        return _RegisteredReceiveBuffer(buffer, registrations, address)

    def _return_receive_buffer(self, scratch: _RegisteredReceiveBuffer) -> None:
        scratch.lease_finalizer = None
        if self._closed:
            self._deregister_registration(scratch.registrations, scratch)
        elif len(self._receive_pool) < self._max_cached_receive_buffers:
            self._receive_pool.append(scratch)
        else:
            smallest_index = min(range(len(self._receive_pool)), key=lambda index: self._receive_pool[index].capacity)
            if scratch.capacity <= self._receive_pool[smallest_index].capacity:
                self._deregister_registration(scratch.registrations, scratch)
                return
            evicted = self._receive_pool[smallest_index]
            self._receive_pool[smallest_index] = scratch
            self._deregister_registration(evicted.registrations, evicted)

    def _release_receive_buffer(self, scratch: _RegisteredReceiveBuffer) -> None:
        with self._lock:
            if self._leased_receive_buffers.pop(id(scratch), None) is not None:
                self._return_receive_buffer(scratch)

    def _prepare_frame_source(self, frame: bytes | bytearray | memoryview) -> _FrameSource:
        view = memoryview(frame)
        if not view.c_contiguous:
            copy_start = time.perf_counter()
            owner: bytearray | memoryview | np.ndarray = bytearray(view)
            self._record_timing("send_source_copy", copy_start)
            address = _buffer_address(owner)
        elif view.readonly:
            owner = np.frombuffer(view, dtype=np.uint8, count=view.nbytes)
            address = int(owner.ctypes.data)
        else:
            owner = view.cast("B")
            address = _buffer_address(owner)

        size = memoryview(owner).nbytes
        if self._is_leased_receive_range(address, size):
            return _FrameSource(owner, None, address, size)

        registration_start = time.perf_counter()
        registrations = self._agent.register_memory(
            [(address, size, 0, "")], mem_type="DRAM", backends=["UCX"]
        )
        self._record_timing("send_source_registration", registration_start)
        if registrations is None:
            raise NixlError("failed to register NIXL source frame")
        return _FrameSource(owner, registrations, address, size)

    def _is_leased_receive_range(self, address: int, size: int) -> bool:
        return any(
            scratch.address <= address and address + size <= scratch.address + scratch.capacity
            for scratch in self._leased_receive_buffers.values()
        )

    def _send_scatter(
        self,
        remote_name: str,
        metadata: bytes,
        frames: tuple[bytes | bytearray | memoryview, ...],
        serialized_remote_descs: bytes,
    ) -> None:
        """Send frames directly to matching registered remote regions."""
        frame_sources: list[_FrameSource] = []
        handle = None
        try:
            with self._lock:
                self._ensure_open()
                previous = self._remote_metadata.get(remote_name)
                if previous != metadata:
                    if previous is not None:
                        self._agent.remove_remote_agent(remote_name)
                        self._remote_metadata.pop(remote_name, None)
                    loaded_name = self._agent.add_remote_agent(metadata)
                    if isinstance(loaded_name, bytes):
                        loaded_name = loaded_name.decode()
                    if loaded_name != remote_name:
                        raise NixlError(
                            f"NIXL remote agent name mismatch: expected {remote_name!r}, got {loaded_name!r}"
                        )
                    self._remote_metadata[remote_name] = metadata

                source_registration_start = time.perf_counter()
                for frame in frames:
                    if memoryview(frame).nbytes:
                        frame_sources.append(self._prepare_frame_source(frame))
                self._record_timing("send_source_registration_total", source_registration_start)
                local_descs = self._agent.get_xfer_descs(
                    [(source.address, source.size, 0) for source in frame_sources],
                    mem_type="DRAM",
                )
                remote_descs = self._agent.deserialize_descs(serialized_remote_descs)
                initialize_start = time.perf_counter()
                handle = self._agent.initialize_xfer("WRITE", local_descs, remote_descs, remote_name, backends=["UCX"])
                self._record_timing("ucx_transfer_initialize", initialize_start)
                transfer_start = time.perf_counter()
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
            self._record_timing("ucx_transfer_wait", transfer_start)
        except NixlError:
            raise
        except Exception as exc:
            raise NixlError(f"NIXL H2H scatter WRITE failed: {exc}") from exc
        finally:
            if handle is not None:
                self._finish_transfer(handle)
            with self._lock:
                for source in frame_sources:
                    if source.registration is not None:
                        self._deregister_registration(source.registration, source)

    def _finish_transfer(self, handle: Any) -> None:
        """Do not release source MRs until NIXL accepts request cleanup."""
        warned = False
        while True:
            try:
                with self._lock:
                    self._agent.release_xfer_handle(handle)
                return
            except Exception as exc:
                if not warned:
                    logger.warning("NIXL transfer cleanup is pending; waiting before releasing its memory: %s", exc)
                    warned = True
                time.sleep(0.01)

    @staticmethod
    def _validate_send_metadata(
        endpoint: dict[str, Any],
        token: dict[str, Any],
        descriptor: Any,
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

    def receive(self, descriptor: Any) -> Future[memoryview]:
        """Complete a receive and lease its registered buffer to the decoded payload."""
        descriptor.validate()
        future: Future[memoryview] = Future()
        with self._lock:
            self._ensure_open()
            state = self._receives.pop(descriptor.transfer_id, None)
            if state is None:
                future.set_exception(NixlError(f"no prepared NIXL receive for {descriptor.transfer_id}"))
                return future
            try:
                lease_start = time.perf_counter()
                # Decoded tensors retain this NumPy exporter through the buffer protocol.
                # Its finalizer prevents reuse while stored or returned data is still alive.
                owner = np.frombuffer(state.scratch.buffer, dtype=np.uint8, count=descriptor.payload_bytes)
                state.scratch.lease_finalizer = weakref.finalize(
                    owner,
                    self._release_receive_buffer,
                    state.scratch,
                )
                self._leased_receive_buffers[id(state.scratch)] = state.scratch
                future.set_result(memoryview(owner))
                self._record_timing("receive_payload_lease", lease_start)
            except Exception as exc:
                self._return_receive_buffer(state.scratch)
                future.set_exception(NixlError(f"failed to lease NIXL receive payload: {exc}"))
        return future

    def cancel_receive(self, transfer_id: str) -> None:
        """Cancel a prepared receive and return its registered buffer to the pool."""
        with self._lock:
            state = self._receives.pop(transfer_id, None)
            if state is None:
                return
            self._return_receive_buffer(state.scratch)

    def close(self) -> None:
        """Stop the send executor and release all NIXL registrations."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._send_executor.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            buffers = list(self._receive_pool)
            prepared_buffers = [state.scratch for state in self._receives.values()]
            for scratch in self._leased_receive_buffers.values():
                if scratch.lease_finalizer is not None:
                    scratch.lease_finalizer.detach()
                buffers.append(scratch)
            for scratch in buffers:
                self._deregister_registration(scratch.registrations, scratch)
            self._receive_pool.clear()
            self._receives.clear()
            self._leased_receive_buffers.clear()
            failed_deregistrations = self._failed_deregistrations
            self._failed_deregistrations = []
            agent = self._agent
            self._agent = None
        # Agent teardown owns prepared MRs because a remote WRITE may still refer to them.
        del agent
        del prepared_buffers
        del failed_deregistrations

    def _ensure_open(self) -> None:
        if self._closed:
            raise NixlError("NIXL runtime is closed")

    def _deregister_registration(self, registrations: Any, owner: Any) -> None:
        try:
            self._agent.deregister_memory(registrations, backends=["UCX"])
        except Exception as exc:
            logger.warning("failed to deregister NIXL memory: %s", exc)
            self._failed_deregistrations.append((owner, registrations))
