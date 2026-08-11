# Copyright 2026 The TransferQueue Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Host payload transport used by :mod:`transfer_queue.storage.simple_storage`.

The public storage API deliberately never exposes UCX objects.  ZMQ remains the
control plane; this module only moves an already encoded, contiguous Host payload.
"""

from __future__ import annotations

import hashlib
from concurrent.futures import CancelledError, Future
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Thread
from time import perf_counter, sleep
from typing import Any, Callable, Literal

from transfer_queue.storage.ucx_discovery import discover_ucx_device
from transfer_queue.utils.logging_utils import get_logger

logger = get_logger(__name__)

DEFAULT_INLINE_THRESHOLD_BYTES = 128 * 1024
DEFAULT_TRANSFER_TIMEOUT_SECONDS: float | None = None
DEFAULT_ENDPOINT_TIMEOUT_SECONDS = 30.0
DEFAULT_CANCEL_TIMEOUT_SECONDS = 5.0


class DataPlaneError(RuntimeError):
    """A data-plane operation could not be completed safely."""


class _UcxRequestFuture(Future[Any]):
    """Future whose cancellation remains effective after a UCP request is posted."""

    def __init__(self) -> None:
        super().__init__()
        self._cancel_requested = Event()

    def cancel(self) -> bool:
        self._cancel_requested.set()
        if super().cancel():
            return True
        return not self.done()

    @property
    def cancel_requested(self) -> bool:
        return self._cancel_requested.is_set()


@dataclass
class _OwnerTask:
    """One operation submitted to the thread-affine UCP worker."""

    kind: Literal["call", "request", "cancel", "stop"]
    operation: Callable[[], Any]
    future: Future[Any]
    poll: Callable[[Any], Any] | None = None
    complete: Callable[[Any], Any] | None = None
    timeout_seconds: float | None = None


@dataclass
class _ActiveRequest:
    """A posted UCP request that the owner thread must keep progressing."""

    request: Any
    future: Future[Any]
    poll: Callable[[Any], Any]
    complete: Callable[[Any], Any]
    deadline: float | None


class _UcxOwnerThread:
    """Own one UCP worker and progress all active requests on one thread."""

    def __init__(self, initialize: Callable[[], None], progress: Callable[[], None]):
        self._tasks: Queue[_OwnerTask] = Queue()
        self._active: list[_ActiveRequest] = []
        self._canceling: list[Any] = []
        self._cancel_waiters: list[Future[None]] = []
        self._progress_callback = progress
        self._progress_sleep_seconds = 50 / 1_000_000
        self._ready = Event()
        self._init_error: BaseException | None = None
        self._thread = Thread(target=self._run, args=(initialize,), name="tq-ucx-owner", daemon=True)
        self._thread.start()
        self._ready.wait()
        if self._init_error is not None:
            raise self._init_error

    def _run(self, initialize: Callable[[], None]) -> None:
        try:
            initialize()
        except BaseException as exc:
            self._init_error = exc
            self._ready.set()
            return
        self._ready.set()
        while True:
            try:
                # Never delay UCP progress behind the control-task queue.  A
                # blocking get is useful only while the worker is idle; with
                # active or canceling requests it added up to 1 ms to every
                # progress iteration and dominated the configured 50 us yield.
                task = self._tasks.get_nowait() if self._active or self._canceling else self._tasks.get(timeout=0.001)
            except Empty:
                task = None
            if task is not None:
                if task.kind == "stop":
                    self._cancel_active_requests(DataPlaneError("UCX owner thread is stopping"))
                    task.future.set_result(None)
                    break
                if task.kind == "cancel":
                    self._cancel_active_requests(DataPlaneError("UCX request canceled during shutdown"))
                    if self._canceling:
                        self._cancel_waiters.append(task.future)
                    else:
                        task.future.set_result(None)
                    continue
                if task.future.set_running_or_notify_cancel():
                    try:
                        if task.kind == "call":
                            task.future.set_result(task.operation())
                        else:
                            request = task.operation()
                            assert task.poll is not None
                            assert task.complete is not None
                            self._active.append(
                                _ActiveRequest(
                                    request=request,
                                    future=task.future,
                                    poll=task.poll,
                                    complete=task.complete,
                                    deadline=(
                                        None if task.timeout_seconds is None else perf_counter() + task.timeout_seconds
                                    ),
                                )
                            )
                    except BaseException as exc:
                        task.future.set_exception(exc)

            self._progress()
            if self._active:
                remaining = []
                for active in self._active:
                    if active.future.cancelled() or (
                        isinstance(active.future, _UcxRequestFuture) and active.future.cancel_requested
                    ):
                        self._start_cancel(active.request)
                        if not active.future.done():
                            active.future.set_exception(CancelledError())
                        continue
                    if active.deadline is not None and perf_counter() >= active.deadline:
                        self._start_cancel(active.request)
                        active.future.set_exception(TimeoutError("UCX request timed out"))
                        continue
                    try:
                        result = active.poll(active.request)
                        if result is None:
                            remaining.append(active)
                        else:
                            active.future.set_result(active.complete(result))
                    except BaseException as exc:
                        # Retire the native request before dropping the logical
                        # request from _active.
                        self._start_cancel(active.request)
                        active.future.set_exception(exc)
                self._active = remaining
                if self._active and self._progress_sleep_seconds:
                    sleep(self._progress_sleep_seconds)
            if self._canceling:
                canceling = []
                for request in self._canceling:
                    try:
                        if request.test_cancel() is None:
                            canceling.append(request)
                    except Exception:
                        # The request is terminal and test_cancel() has already
                        # released its native handle before reporting failure.
                        pass
                self._canceling = canceling
                if not canceling:
                    waiters, self._cancel_waiters = self._cancel_waiters, []
                    for waiter in waiters:
                        if not waiter.done():
                            waiter.set_result(None)
                elif not self._active and self._progress_sleep_seconds:
                    sleep(self._progress_sleep_seconds)

    def _start_cancel(self, request: Any) -> None:
        """Begin native cancellation and retain the request until terminal."""
        try:
            request.start_cancel()
            self._canceling.append(request)
        except Exception:
            # A failed start means there is no safe action left for this
            # best-effort cleanup path. Normal request failures are delivered
            # through the caller-facing Future before reaching here.
            pass

    def _cancel_active_requests(self, error: BaseException) -> None:
        """Cancel native requests before their worker/context can be destroyed."""
        active, self._active = self._active, []
        for request in active:
            self._start_cancel(request.request)
            if not request.future.done():
                request.future.set_exception(error)

    def _progress(self) -> None:
        try:
            self._progress_callback()
        except Exception:
            # A foreground operation reports request failures to its caller.
            pass

    def call(self, operation: Callable[[], Any]) -> Any:
        return self.submit(operation).result()

    def submit(self, operation: Callable[[], Any]) -> Future[Any]:
        """Queue an owner-thread operation without blocking the caller."""
        future: Future[Any] = Future()
        self._tasks.put(_OwnerTask(kind="call", operation=operation, future=future))
        return future

    def submit_request(
        self,
        operation: Callable[[], Any],
        poll: Callable[[Any], Any],
        complete: Callable[[Any], Any] | None,
        timeout_seconds: float | None,
    ) -> Future[Any]:
        """Post a native request and poll it alongside other in-flight requests."""
        future: Future[Any] = _UcxRequestFuture()
        # Keeping both callbacks in the owner thread avoids calling
        # thread-affine UCP request methods from executor threads.
        self._tasks.put(
            _OwnerTask(
                kind="request",
                operation=operation,
                future=future,
                poll=poll,
                complete=complete or (lambda value: value),
                timeout_seconds=timeout_seconds,
            )
        )
        return future

    def cancel_active_requests(self) -> None:
        """Synchronously cancel all active native requests on the owner thread."""
        future: Future[None] = Future()
        self._tasks.put(_OwnerTask(kind="cancel", operation=lambda: None, future=future))
        future.result()

    def stop(self) -> None:
        future: Future[None] = Future()
        self._tasks.put(_OwnerTask(kind="stop", operation=lambda: None, future=future))
        future.result()
        self._thread.join()


@dataclass(frozen=True)
class PayloadDescriptor:
    """Control-plane description of exactly one encoded payload."""

    transfer_id: str
    tag: int
    payload_bytes: int
    frame_count: int

    def validate_identity(self) -> None:
        """Validate fields that identify a transfer before its size is known."""
        if not self.transfer_id or len(self.transfer_id) > 128:
            raise DataPlaneError("UCX transfer_id must contain 1 to 128 characters")
        expected_tag = transfer_tag(self.transfer_id)
        if self.tag != expected_tag:
            raise DataPlaneError(f"UCX tag mismatch for {self.transfer_id}: expected {expected_tag}, got {self.tag}")

    def validate(self) -> None:
        """Reject malformed control metadata before native allocation or I/O."""
        self.validate_identity()
        if self.payload_bytes < 0:
            raise DataPlaneError(f"negative payload length for {self.transfer_id}")
        if self.frame_count <= 0:
            raise DataPlaneError(f"frame_count must be positive for {self.transfer_id}")

    def to_dict(self) -> dict[str, int | str | list[int]]:
        result: dict[str, int | str | list[int]] = {
            "transfer_id": self.transfer_id,
            "tag": self.tag,
            "payload_bytes": self.payload_bytes,
            "frame_count": self.frame_count,
        }
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PayloadDescriptor:
        return cls(
            transfer_id=str(value["transfer_id"]),
            tag=int(value["tag"]),
            payload_bytes=int(value["payload_bytes"]),
            frame_count=int(value["frame_count"]),
        )


def transfer_tag(transfer_id: str) -> int:
    """Derive a stable positive 63-bit UCX tag from a UUID-like transfer id."""
    digest = hashlib.blake2b(transfer_id.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") & ((1 << 63) - 1)


def address_digest(address: bytes) -> str:
    """Return the digest used to bind a UCX address to bootstrap metadata."""
    if not isinstance(address, bytes):
        raise DataPlaneError("UCX worker address must be bytes")
    return hashlib.sha256(address).hexdigest()


class UcxDataPlane:
    """Thin synchronous/future wrapper over the native UCP Tagged binding.

    Synchronous calls are available for setup and small standalone tools. Large
    transfer paths use the Future-returning methods, which keep UCP progress on
    the owner thread without blocking the caller's asyncio loop.
    """

    provider = "ucx"

    def __init__(
        self,
        timeout_seconds: float | None,
        require_address_digest: bool = True,
        ucx_config: dict[str, str] | None = None,
    ):
        self._timeout_seconds = timeout_seconds
        self._require_address_digest = require_address_digest
        self._ucx_config = ucx_config or {}
        # Keep the complete native object graph on one owner thread; callers
        # may use this facade from asyncio or Ray/ZMQ worker threads.
        self._owner = _UcxOwnerThread(self._init_native, lambda: self._native.progress())
        self._endpoints: dict[bytes, Any] = {}
        self._receives: dict[str, Any] = {}
        self._closed = False

    def _init_native(self) -> None:
        try:
            from transfer_queue import _ucx
        except ImportError as exc:  # pragma: no cover - depends on optional native build
            raise DataPlaneError("the installed TransferQueue package does not include UCX support") from exc
        self._native = _ucx.Worker(self._ucx_config)
        # A worker address is immutable for the lifetime of its worker. Cache
        # it once on the owner thread instead of enqueueing a native call for
        # every GET request and bootstrap read.
        self._address = self._native.address()

    def _call(self, operation: Callable[[], Any]) -> Any:
        if self._closed:
            raise DataPlaneError("UCX data plane is closed")
        return self._owner.call(operation)

    @property
    def address(self) -> bytes:
        if self._closed:
            raise DataPlaneError("UCX data plane is closed")
        return self._address

    def prepare_receive(self, descriptor: PayloadDescriptor) -> None:
        self._validate_descriptor(descriptor)

        def operation() -> None:
            if descriptor.transfer_id in self._receives:
                raise DataPlaneError(f"duplicate receive preparation: {descriptor.transfer_id}")
            request = self._native.post_receive(descriptor.tag, descriptor.payload_bytes)
            self._receives[descriptor.transfer_id] = request

        self._call(operation)

    def send(
        self,
        peer_address: bytes,
        descriptor: PayloadDescriptor,
        payload: bytes | bytearray | memoryview,
        peer_address_digest: str | None = None,
    ) -> None:
        self._validate_send(peer_address, descriptor, len(payload), peer_address_digest)

        def operation() -> None:
            request = self._post_send(peer_address, peer_address_digest, descriptor, payload)
            request.wait(self._timeout_seconds)

        self._call(operation)

    def send_async(
        self,
        peer_address: bytes,
        descriptor: PayloadDescriptor,
        payload: bytes | bytearray | memoryview,
        peer_address_digest: str | None = None,
    ) -> Future[None]:
        """Start a send without blocking the caller's control-plane handler.

        The future owns the payload through the submitted operation. UCX
        posting, progress, completion polling, and endpoint access all remain
        on the single owner thread; multiple requests can be in flight without
        serializing on one blocking ``Request.wait()``.
        """
        if self._closed:
            raise DataPlaneError("UCX data plane is closed")
        self._validate_send(peer_address, descriptor, len(payload), peer_address_digest)

        def operation() -> Any:
            return self._post_send(peer_address, peer_address_digest, descriptor, payload)

        return self._owner.submit_request(
            operation,
            lambda request: request.test(),
            lambda _: None,
            self._timeout_seconds,
        )

    def warmup(self, peer_address: bytes, peer_address_digest: str | None = None) -> None:
        """Create and flush a cached endpoint before the first payload send."""
        if self._require_address_digest and peer_address_digest is None:
            raise DataPlaneError("UCX worker address digest is required")

        def operation() -> None:
            endpoint = self._endpoint(peer_address, peer_address_digest)
            endpoint.flush(DEFAULT_ENDPOINT_TIMEOUT_SECONDS)

        self._call(operation)

    def _finish_receive_operation(self, descriptor: PayloadDescriptor) -> memoryview:
        try:
            request = self._receives.pop(descriptor.transfer_id)
        except KeyError as exc:
            raise DataPlaneError(f"receive was not prepared: {descriptor.transfer_id}") from exc
        return self._received_payload(descriptor, request.wait(self._timeout_seconds))

    def finish_receive(self, descriptor: PayloadDescriptor) -> memoryview:
        return self._call(lambda: self._finish_receive_operation(descriptor))

    def finish_receive_future(self, descriptor: PayloadDescriptor) -> Future[memoryview]:
        """Complete a receive on the UCX owner thread without a Python worker thread."""
        if self._closed:
            raise DataPlaneError("UCX data plane is closed")

        def operation() -> Any:
            try:
                return self._receives.pop(descriptor.transfer_id)
            except KeyError as exc:
                raise DataPlaneError(f"receive was not prepared: {descriptor.transfer_id}") from exc

        def complete(payload: Any) -> memoryview:
            return self._received_payload(descriptor, payload)

        return self._owner.submit_request(operation, lambda request: request.test(), complete, self._timeout_seconds)

    def cancel_receive(self, transfer_id: str) -> Future[None]:
        """Start canceling a posted receive without blocking the control plane."""
        if self._closed:
            raise DataPlaneError("UCX data plane is closed")

        def operation() -> Any:
            request = self._receives.pop(transfer_id, None)
            if request is not None:
                request.start_cancel()
            return request

        future = self._owner.submit_request(
            operation,
            lambda request: True if request is None else request.test_cancel(),
            lambda _: None,
            DEFAULT_CANCEL_TIMEOUT_SECONDS,
        )

        def report_cancel_failure(completed: Future[None]) -> None:
            if completed.cancelled():
                return
            try:
                completed.result()
            except Exception:
                # Cancellation is best effort. The owner thread still applies
                # its request timeout and keeps the native request alive until
                # UCX reports a terminal state.
                pass

        future.add_done_callback(report_cancel_failure)
        return future

    @property
    def pending_receive_count(self) -> int:
        """Return the number of posted, not-yet-finished receives."""
        return self._call(lambda: len(self._receives))

    def close(self) -> None:
        if self._closed:
            return
        # Refuse new calls before enqueueing shutdown work. Existing requests
        # are canceled below on the owner thread before the native worker dies.
        self._closed = True

        def operation() -> None:
            for transfer_id in list(self._receives):
                request = self._receives.pop(transfer_id)
                request.cancel()
            for endpoint in self._endpoints.values():
                endpoint.close(DEFAULT_ENDPOINT_TIMEOUT_SECONDS)
            self._endpoints.clear()
            native = self._native
            native.close()
            # Keep destruction on the UCX owner thread as well.  The binding
            # owns UCP objects whose cleanup is thread-sensitive on HNS.
            self._native = None

        try:
            self._owner.cancel_active_requests()
            self._owner.call(operation)
        finally:
            self._owner.stop()

    def _validate_send(
        self,
        peer_address: bytes,
        descriptor: PayloadDescriptor,
        payload_bytes: int,
        peer_address_digest: str | None,
    ) -> None:
        self._validate_descriptor(descriptor)
        if payload_bytes != descriptor.payload_bytes:
            raise DataPlaneError(
                f"payload length mismatch for {descriptor.transfer_id}: "
                f"expected {descriptor.payload_bytes}, got {payload_bytes}"
            )
        if self._require_address_digest and peer_address_digest is None:
            raise DataPlaneError("UCX worker address digest is required")
        self._validate_peer_address(peer_address, peer_address_digest)

    def _validate_descriptor(self, descriptor: PayloadDescriptor) -> None:
        descriptor.validate()

    def _post_send(
        self,
        peer_address: bytes,
        peer_address_digest: str | None,
        descriptor: PayloadDescriptor,
        payload: bytes | bytearray | memoryview,
    ) -> Any:
        endpoint = self._endpoint(peer_address, peer_address_digest)
        return endpoint.post_send(descriptor.tag, memoryview(payload))

    @staticmethod
    def _received_payload(descriptor: PayloadDescriptor, received: Any) -> memoryview:
        """Validate the single logical buffer filled by one or more receives."""
        payload = memoryview(received)
        if payload.nbytes != descriptor.payload_bytes:
            raise DataPlaneError(
                f"received length mismatch for {descriptor.transfer_id}: "
                f"expected {descriptor.payload_bytes}, got {payload.nbytes}"
            )
        return payload

    @staticmethod
    def _validate_peer_address(peer_address: bytes, expected_digest: str | None = None) -> None:
        """Validate Python control metadata before queueing native endpoint work."""
        if not isinstance(peer_address, bytes):
            raise DataPlaneError("UCX worker address must be bytes")
        if expected_digest is not None and address_digest(peer_address) != expected_digest:
            raise DataPlaneError("UCX worker address digest does not match bootstrap metadata")
        # This is a corruption guard, not native address parsing.
        if not 16 <= len(peer_address) <= 1024 * 1024:
            raise DataPlaneError(f"invalid UCX worker address length: {len(peer_address)}")

    def _endpoint(self, peer_address: bytes, expected_digest: str | None = None):
        self._validate_peer_address(peer_address, expected_digest)
        endpoint = self._endpoints.get(peer_address)
        if endpoint is None:
            endpoint = self._native.connect(peer_address)
            self._endpoints[peer_address] = endpoint
        return endpoint


def create_data_plane(enabled: bool, local_ip: str | None = None) -> UcxDataPlane | None:
    """Create the optional UCX payload transport, falling back to ZMQ safely."""
    if not enabled:
        return None
    try:
        selection = discover_ucx_device(local_ip)
        if selection.rdma_device is None:
            raise DataPlaneError(f"no RoCE-v2 device and GID match local IP {local_ip or 'unknown'}")
        transport = UcxDataPlane(
            timeout_seconds=DEFAULT_TRANSFER_TIMEOUT_SECONDS,
            require_address_digest=True,
            ucx_config=selection.ucx_config,
        )
        logger.info(
            "SimpleStorage payload transport enabled: provider=ucx local_ip=%s device=%s gid_index=%s tls=%s",
            local_ip or "auto",
            selection.net_devices,
            selection.gid_index,
            selection.ucx_config.get("TLS", "ucx-default"),
        )
        return transport
    except Exception as exc:
        logger.warning(
            "SimpleStorage UCX payload transport is unavailable; falling back to ZMQ: %s: %s",
            type(exc).__name__,
            exc,
        )
        return None
