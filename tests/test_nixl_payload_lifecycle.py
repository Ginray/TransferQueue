# Copyright 2025 Huawei Technologies Co., Ltd. All Rights Reserved.
# Copyright 2025 The TransferQueue Team

"""Focused tests for the frame-native NIXL payload lifecycle."""

from __future__ import annotations

import asyncio
import gc
import socket
import sys
import threading
import time
import types
from concurrent.futures import Future
from contextlib import ExitStack
from queue import SimpleQueue
from typing import Any, Callable

import numpy as np
import pytest
import torch
import zmq

from transfer_queue.storage.payload_transfer import DeferredResponse
from transfer_queue.storage.payload_transfer.nixl import (
    NixlPayloadTransfer,
    PayloadDescriptor,
    ReceiveToken,
    TransferEndpoint,
    _PendingGet,
)
from transfer_queue.storage.payload_transfer.nixl_ucx_runtime import (
    DEFAULT_NIXL_RECEIVE_BUFFER_CACHE_BYTES,
    NixlError,
    NixlRuntime,
    NixlWriteNotStarted,
    _frame_regions,
    _FrameSource,
    _RegisteredReceiveBuffer,
)
from transfer_queue.storage.simple_storage import _drain_deferred_responses, _queue_deferred_response
from transfer_queue.utils.serial_utils import decode, encode
from transfer_queue.utils.zmq_utils import ZMQMessage, ZMQRequestType


class _FakeAgent:
    def __init__(self) -> None:
        self.backends = {"UCX": object()}
        self.register_calls: list[Any] = []
        self.deregister_calls: list[Any] = []
        self.events: list[str] = []
        self.add_remote_calls: list[bytes] = []
        self.remove_remote_calls: list[str] = []
        self.transfer_calls: list[Any] = []
        self.metadata_names: dict[bytes, str] = {}
        self.status_by_remote: dict[str, str] = {}
        self.release_error = False
        self.deregister_error = False
        self._registration_id = 0
        self._handle_id = 0

    def get_agent_metadata(self) -> bytes:
        return f"metadata-{len(self.register_calls)}".encode()

    def register_memory(self, regions: Any, **kwargs: Any) -> tuple[str, int]:
        self.events.append("register")
        self.register_calls.append(regions)
        self._registration_id += 1
        return ("registration", self._registration_id)

    def deregister_memory(self, registration: Any, **kwargs: Any) -> None:
        self.events.append("deregister")
        if self.deregister_error:
            raise RuntimeError("deregister failed")
        self.deregister_calls.append(registration)

    def get_xfer_descs(self, regions: Any, **kwargs: Any) -> Any:
        return list(regions)

    def get_serialized_descs(self, descs: Any) -> bytes:
        return repr(descs).encode()

    def deserialize_descs(self, descs: bytes) -> bytes:
        return descs

    def add_remote_agent(self, metadata: bytes) -> str:
        self.add_remote_calls.append(metadata)
        return self.metadata_names[metadata]

    def remove_remote_agent(self, remote_name: str) -> None:
        self.remove_remote_calls.append(remote_name)

    def initialize_xfer(self, operation: str, local: Any, remote: Any, remote_name: str, **kwargs: Any) -> Any:
        self._handle_id += 1
        return (self._handle_id, remote_name, local, remote)

    def transfer(self, handle: Any) -> str:
        self.transfer_calls.append(handle)
        return self.status_by_remote.get(handle[1], "DONE")

    def check_xfer_state(self, handle: Any) -> str:
        return self.status_by_remote.get(handle[1], "DONE")

    def release_xfer_handle(self, handle: Any) -> None:
        if self.release_error:
            raise RuntimeError("release failed")


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    agents: list[_FakeAgent] = []
    module = types.ModuleType("nixl")

    def make_agent(*args: Any, **kwargs: Any) -> _FakeAgent:
        agent = _FakeAgent()
        agents.append(agent)
        return agent

    module.nixl_agent = make_agent
    module.nixl_agent_config = lambda **kwargs: kwargs
    monkeypatch.setitem(sys.modules, "nixl", module)
    instance = NixlRuntime(
        receive_buffer_cache_bytes=getattr(request, "param", DEFAULT_NIXL_RECEIVE_BUFFER_CACHE_BYTES)
    )
    yield instance, agents[0]
    instance.close()


def _descriptor(transfer_id: str, *sizes: int) -> PayloadDescriptor:
    return PayloadDescriptor(transfer_id, sum(sizes), tuple(sizes))


def _token(remote_name: str, metadata: bytes, descriptor: PayloadDescriptor) -> dict[str, Any]:
    return {
        "agent_name": remote_name,
        "agent_metadata": metadata,
        "frame_remote_descs": b"remote-descs",
        "payload_bytes": descriptor.payload_bytes,
    }


def test_frame_native_layout_preserves_empty_frames_and_large_offsets():
    sizes = (3 * 1024**3, 0, 3 * 1024**3, 2 * 1024**3)
    descriptor = _descriptor("large", *sizes)
    descriptor.validate()

    address = 0x1000
    regions = _frame_regions(address, descriptor.frame_sizes)

    assert descriptor.payload_bytes == 8 * 1024**3
    assert regions == [
        (address, 3 * 1024**3, 0),
        (address + 3 * 1024**3, 3 * 1024**3, 0),
        (address + 6 * 1024**3, 2 * 1024**3, 0),
    ]


def test_all_empty_frames_skip_registration_and_write(runtime):
    instance, agent = runtime
    descriptor = _descriptor("empty", 0, 0)

    token = instance.prepare_receive(descriptor)
    frames = instance.receive(descriptor).result()
    sent = instance.send({"agent_name": "peer"}, token, descriptor, (b"", b""))

    assert [bytes(frame) for frame in frames] == [b"", b""]
    assert sent.result() is None
    assert agent.register_calls == []
    assert agent.transfer_calls == []


@pytest.mark.parametrize("runtime", [0, DEFAULT_NIXL_RECEIVE_BUFFER_CACHE_BYTES], indirect=True)
def test_receive_frames_decode_without_copy_and_lease_until_last_tensor(runtime):
    instance, agent = runtime
    source = {"value": torch.arange(8, dtype=torch.int32)}
    encoded = tuple(encode(source))
    descriptor = _descriptor("receive", *(memoryview(frame).nbytes for frame in encoded))

    instance.prepare_receive(descriptor)
    scratch = instance._receives[descriptor.transfer_id]
    assert scratch is not None
    offset = 0
    for frame in encoded:
        view = memoryview(frame)
        scratch.buffer[offset : offset + view.nbytes] = view
        offset += view.nbytes

    frames = instance.receive(descriptor).result()
    decoded = decode(list(frames))
    detached = decoded["value"].detach()
    del frames, decoded
    gc.collect()

    assert instance._idle_receive_buffers == []
    assert agent.deregister_calls == []
    assert torch.equal(detached, source["value"])

    del detached
    gc.collect()
    if instance.diagnostics["receive_buffer_cache_bytes"] == 0:
        assert instance._idle_receive_buffers == []
        assert agent.deregister_calls == [scratch.registration]
        assert instance.diagnostics["registered_bytes"] == 0
    else:
        assert instance._idle_receive_buffers == [scratch]
        assert instance.diagnostics["idle_receive_bytes"] == scratch.capacity


def test_receive_pool_reuses_an_idle_buffer_that_fits(runtime):
    instance, agent = runtime
    first = _descriptor("first", 16)
    second = _descriptor("second", 8)

    instance.prepare_receive(first)
    first_buffer = instance._receives["first"]
    instance.cancel_receive("first")
    instance.prepare_receive(second)

    assert instance._receives["second"] is first_buffer
    assert len(agent.register_calls) == 1
    assert agent.deregister_calls == []
    assert instance.diagnostics["receive_buffer_acquisitions"] == 2
    assert instance.diagnostics["receive_buffer_registrations"] == 1
    assert instance.diagnostics["receive_buffer_reuses"] == 1


def test_receive_pool_best_fit_prefers_smallest_sufficient_idle_buffer(runtime):
    instance, agent = runtime
    smaller = _descriptor("smaller", 16)
    larger = _descriptor("larger", 24)

    instance.prepare_receive(smaller)
    smaller_buffer = instance._receives[smaller.transfer_id]
    instance.prepare_receive(larger)
    larger_buffer = instance._receives[larger.transfer_id]
    instance.cancel_receive(smaller.transfer_id)
    instance.cancel_receive(larger.transfer_id)

    request = _descriptor("request", 12)
    instance.prepare_receive(request)

    assert instance._receives[request.transfer_id] is smaller_buffer
    assert instance._receives[request.transfer_id] is not larger_buffer
    assert len(agent.register_calls) == 2
    assert agent.deregister_calls == []
    assert instance.diagnostics["receive_buffer_reuses"] == 1


def test_receive_pool_tracks_pending_and_leased_working_set(runtime):
    instance, _ = runtime
    leased = _descriptor("leased", 16)
    pending = _descriptor("pending", 24)

    instance.prepare_receive(leased)
    held_frames = instance.receive(leased).result()
    instance.prepare_receive(pending)

    diagnostics = instance.diagnostics
    assert diagnostics["receive_working_set_hwm_count"] == 2
    assert diagnostics["receive_working_set_hwm_bytes"] == 40

    instance.cancel_receive(pending.transfer_id)
    del held_frames
    gc.collect()
    assert instance.diagnostics["idle_receive_bytes"] == 40
    assert instance.diagnostics["pending_receive_bytes"] == 0
    assert instance.diagnostics["leased_receive_bytes"] == 0


@pytest.mark.parametrize("runtime", [32], indirect=True)
def test_receive_pool_evicts_oldest_idle_before_registering_on_miss(runtime):
    instance, agent = runtime
    leased = _descriptor("leased", 16)
    instance.prepare_receive(leased)
    leased_buffer = instance._receives[leased.transfer_id]
    held_frames = instance.receive(leased).result()

    undersized = _descriptor("undersized", 8)
    instance.prepare_receive(undersized)
    undersized_buffer = instance._receives[undersized.transfer_id]

    larger_idle = _descriptor("larger_idle", 24)
    instance.prepare_receive(larger_idle)
    larger_idle_buffer = instance._receives[larger_idle.transfer_id]
    instance.cancel_receive(undersized.transfer_id)
    instance.cancel_receive(larger_idle.transfer_id)

    request = _descriptor("request", 32)
    instance.prepare_receive(request)

    assert instance._leased_receive_buffers[id(leased_buffer)] is leased_buffer
    assert instance._receives[request.transfer_id].capacity == 32
    assert agent.deregister_calls == [
        undersized_buffer.registration,
        larger_idle_buffer.registration,
    ]
    assert agent.events[-3:] == ["deregister", "deregister", "register"]
    assert instance._idle_receive_buffers == []
    assert instance.diagnostics["registered_bytes"] == 16 + 32
    assert instance.diagnostics["receive_working_set_hwm_count"] == 3
    assert instance.diagnostics["receive_working_set_hwm_bytes"] == 48
    assert instance.diagnostics["receive_buffer_acquisitions"] == 4
    assert instance.diagnostics["receive_buffer_registrations"] == 4
    assert instance.diagnostics["receive_buffer_reuses"] == 0
    assert instance.diagnostics["receive_buffer_evictions"] == 2

    instance.cancel_receive(request.transfer_id)
    del held_frames


@pytest.mark.parametrize("runtime", [16], indirect=True)
def test_receive_pool_releases_excess_on_return_without_another_prepare(runtime):
    instance, agent = runtime
    first = _descriptor("first", 16)
    second = _descriptor("second", 16)
    instance.prepare_receive(first)
    instance.prepare_receive(second)
    second_buffer = instance._receives[second.transfer_id]

    instance.cancel_receive(first.transfer_id)
    instance.cancel_receive(second.transfer_id)

    assert instance.diagnostics["idle_receive_bytes"] == 16
    assert instance.diagnostics["registered_bytes"] == 16
    assert instance.diagnostics["pending_receive_buffers"] == 0
    assert agent.deregister_calls == [second_buffer.registration]


@pytest.mark.parametrize("runtime", [64], indirect=True)
def test_receive_pool_does_not_lease_an_oversized_idle_buffer(runtime):
    instance, agent = runtime
    large = _descriptor("large", 64)
    instance.prepare_receive(large)
    large_buffer = instance._receives[large.transfer_id]
    instance.cancel_receive(large.transfer_id)

    small = _descriptor("small", 8)
    instance.prepare_receive(small)

    assert instance._receives[small.transfer_id].capacity == 8
    assert instance._receives[small.transfer_id] is not large_buffer
    assert agent.deregister_calls == [large_buffer.registration]
    assert instance.diagnostics["receive_buffer_reuses"] == 0


@pytest.mark.parametrize("runtime", [16], indirect=True)
def test_receive_larger_than_cache_preserves_live_data_and_warm_idle_buffer(runtime):
    instance, agent = runtime
    warm = _descriptor("warm", 8)
    instance.prepare_receive(warm)
    warm_buffer = instance._receives[warm.transfer_id]
    instance.cancel_receive(warm.transfer_id)

    large = _descriptor("large", 32)
    instance.prepare_receive(large)
    large_buffer = instance._receives[large.transfer_id]
    large_buffer.buffer[:] = b"x" * 32
    frames = instance.receive(large).result()

    assert bytes(frames[0]) == b"x" * 32
    assert instance._idle_receive_buffers == [warm_buffer]
    assert agent.deregister_calls == []

    del frames
    gc.collect()

    assert agent.deregister_calls == [large_buffer.registration]
    assert instance._idle_receive_buffers == [warm_buffer]
    assert instance.diagnostics["idle_receive_bytes"] == 8
    assert instance.diagnostics["registered_bytes"] == 8


@pytest.mark.parametrize("runtime", [0], indirect=True)
def test_receive_prepare_failure_releases_uncached_buffer(runtime, monkeypatch):
    instance, agent = runtime

    def fail_metadata():
        raise RuntimeError("metadata failed")

    monkeypatch.setattr(agent, "get_agent_metadata", fail_metadata)
    with pytest.raises(NixlError, match="metadata failed"):
        instance.prepare_receive(_descriptor("failed", 16))

    assert instance._receives == {}
    assert instance._idle_receive_buffers == []
    assert len(agent.deregister_calls) == 1
    assert instance.diagnostics["registered_bytes"] == 0


@pytest.mark.parametrize("runtime", [0], indirect=True)
@pytest.mark.parametrize("failing_call", ["np.frombuffer", "_frame_views"])
def test_receive_exposure_failure_keeps_pending_buffer_owned(runtime, monkeypatch, failing_call):
    instance, agent = runtime
    descriptor = _descriptor("pending", 16)
    instance.prepare_receive(descriptor)
    scratch = instance._receives[descriptor.transfer_id]

    def fail_owner(*args, **kwargs):
        raise RuntimeError("view creation failed")

    monkeypatch.setattr(f"transfer_queue.storage.payload_transfer.nixl_ucx_runtime.{failing_call}", fail_owner)
    with pytest.raises(NixlError, match="view creation failed"):
        instance.receive(descriptor).result()

    assert instance._receives[descriptor.transfer_id] is scratch
    assert instance._leased_receive_buffers == {}
    assert agent.deregister_calls == []
    instance.cancel_receive(descriptor.transfer_id)
    assert agent.deregister_calls == [scratch.registration]
    assert instance.diagnostics["registered_bytes"] == 0


@pytest.mark.parametrize("runtime", [0, 16], indirect=True)
def test_receive_cleanup_failure_retains_memory_and_stops_new_allocations(runtime):
    instance, agent = runtime
    descriptor = _descriptor("failed", 16)
    instance.prepare_receive(descriptor)
    scratch = instance._receives[descriptor.transfer_id]
    agent.deregister_error = True

    instance.cancel_receive(descriptor.transfer_id)

    with pytest.raises(NixlError, match="receive memory cleanup failed"):
        instance.prepare_receive(_descriptor("next", 4))
    assert instance._quarantined_receive_buffers == [scratch]
    assert instance.diagnostics["quarantined_bytes"] == 16
    assert instance.diagnostics["registered_bytes"] == 16
    assert instance.diagnostics["idle_receive_bytes"] == 0
    assert len(agent.register_calls) == 1


@pytest.mark.parametrize("runtime", [0], indirect=True)
def test_sources_reuse_leased_mr_and_external_registration_is_transfer_scoped(runtime):
    instance, agent = runtime
    descriptor = _descriptor("lease", 8)
    instance.prepare_receive(descriptor)
    leased_frame = instance.receive(descriptor).result()[0]

    reused = instance._acquire_frame_source(leased_frame)
    writable = bytearray(b"writable")
    direct = instance._acquire_frame_source(writable)
    readonly = instance._acquire_frame_source(b"readonly")
    non_contiguous = instance._acquire_frame_source(memoryview(np.arange(8, dtype=np.uint8))[::2])
    direct_again = instance._acquire_frame_source(writable)

    assert reused.registration is None
    assert isinstance(direct.owner, memoryview)
    assert isinstance(readonly.owner, bytearray)
    assert isinstance(non_contiguous.owner, bytearray)
    assert isinstance(direct_again.owner, bytearray)
    assert direct_again.address != direct.address
    assert direct.registration != direct_again.registration
    assert len(agent.register_calls) == 5  # one receive MR plus four transfer-local registrations


def test_full_metadata_reused_then_replaced_in_peer_executor(runtime):
    instance, agent = runtime
    descriptor = _descriptor("send", 1)
    agent.metadata_names.update({b"m1": "peer", b"m2": "peer"})

    instance.send({}, _token("peer", b"m1", descriptor), descriptor, (bytearray(b"a"),)).result()
    instance.send({}, _token("peer", b"m1", descriptor), descriptor, (bytearray(b"b"),)).result()
    instance.send({}, _token("peer", b"m2", descriptor), descriptor, (bytearray(b"c"),)).result()

    assert agent.add_remote_calls == [b"m1", b"m2"]
    assert agent.remove_remote_calls == ["peer"]


def test_same_peer_serializes_while_different_peer_runs_concurrently(runtime, monkeypatch: pytest.MonkeyPatch):
    instance, _ = runtime
    descriptor = _descriptor("concurrency", 1)
    first_started = threading.Event()
    other_started = threading.Event()
    release = threading.Event()
    starts: list[str] = []

    def blocking_send(remote_name: str, *args: Any) -> None:
        starts.append(remote_name)
        (first_started if remote_name == "peer-a" else other_started).set()
        release.wait(timeout=2)

    monkeypatch.setattr(instance, "_send_scatter", blocking_send)
    first = instance.send({}, _token("peer-a", b"a", descriptor), descriptor, (bytearray(b"a"),))
    assert first_started.wait(timeout=1)
    queued = instance.send({}, _token("peer-a", b"a", descriptor), descriptor, (bytearray(b"b"),))
    other = instance.send({}, _token("peer-b", b"b", descriptor), descriptor, (bytearray(b"c"),))

    assert other_started.wait(timeout=1)
    assert starts.count("peer-a") == 1
    release.set()
    first.result(timeout=1)
    queued.result(timeout=1)
    other.result(timeout=1)
    assert starts.count("peer-a") == 2


def test_completed_write_cleanup_failure_keeps_success_and_fails_peer(runtime):
    instance, agent = runtime
    descriptor = _descriptor("cleanup", 1)
    agent.metadata_names[b"metadata"] = "peer"
    agent.release_error = True

    completed = instance.send({}, _token("peer", b"metadata", descriptor), descriptor, (bytearray(b"x"),))

    assert completed.result(timeout=1) is None
    assert instance._retained_resources
    with pytest.raises(NixlError, match="has failed"):
        instance.send({}, _token("peer", b"metadata", descriptor), descriptor, (bytearray(b"y"),))


def test_sender_reuses_idle_executor_without_rebinding_outstanding_peer(runtime, monkeypatch):
    instance, _ = runtime
    descriptor = _descriptor("reuse-executor", 1)
    started = threading.Event()
    release = threading.Event()

    def send(remote_name, *args):
        if remote_name == "peer-a":
            started.set()
            release.wait(timeout=2)

    monkeypatch.setattr(instance, "_send_scatter", send)
    first = instance.send({}, _token("peer-a", b"a", descriptor), descriptor, (b"x",))
    try:
        assert started.wait(timeout=1)
        first_sender = instance._peer_senders["peer-a"]
        queued = instance.send({}, _token("peer-a", b"a", descriptor), descriptor, (b"x",))
        assert queued.cancel()
        assert first_sender.outstanding == 1
        other = instance.send({}, _token("peer-b", b"b", descriptor), descriptor, (b"x",))
        assert instance._peer_senders["peer-b"] is not first_sender
        other.result(timeout=1)
    finally:
        release.set()
    first.result(timeout=1)
    # A barrier also waits for the executor's Future completion callbacks.
    first_sender.executor.submit(lambda: None).result(timeout=1)

    instance.send({}, _token("next-epoch", b"c", descriptor), descriptor, (b"x",)).result(timeout=1)
    assert instance._peer_senders["next-epoch"] is first_sender
    assert "peer-a" not in instance._peer_senders
    assert instance.diagnostics["sender_executors"] == 2


@pytest.mark.parametrize("outcome", ["success", "failure", "cancelled", "submit_error"])
def test_sender_retires_immediate_completion_and_submit_failure(runtime, monkeypatch, outcome):
    instance, agent = runtime
    descriptor = _descriptor("retire", 1)
    agent.metadata_names[b"metadata"] = "peer"
    token = _token("peer", b"metadata", descriptor)
    instance.send({}, token, descriptor, (b"x",)).result(timeout=1)
    sender = instance._peer_senders["peer"]
    sender.executor.submit(lambda: None).result(timeout=1)
    completed = Future()
    if outcome == "success":
        completed.set_result(None)
    elif outcome == "failure":
        completed.set_exception(NixlError("write failed"))
    else:
        completed.cancel()

    def submit(*args):
        if outcome == "submit_error":
            raise RuntimeError("submit failed")
        return completed

    monkeypatch.setattr(sender.executor, "submit", submit)
    if outcome == "submit_error":
        with pytest.raises(RuntimeError, match="submit failed"):
            instance.send({}, token, descriptor, (b"x",))
    else:
        assert instance.send({}, token, descriptor, (b"x",)) is completed
    assert instance.diagnostics["outstanding_sends"] == 0


def test_send_deadline_covers_queue_and_source_preparation(runtime, monkeypatch):
    instance, agent = runtime
    descriptor = _descriptor("deadline", 1)
    agent.metadata_names[b"metadata"] = "peer"
    token = _token("peer", b"metadata", descriptor)
    clock = [0.0]
    monkeypatch.setattr(
        "transfer_queue.storage.payload_transfer.nixl_ucx_runtime.time",
        types.SimpleNamespace(monotonic=lambda: clock[0], sleep=time.sleep, time_ns=time.time_ns),
    )
    started = threading.Event()
    release = threading.Event()
    acquire = instance._acquire_frame_source

    def prepare(frame):
        started.set()
        release.wait(timeout=2)
        return acquire(frame)

    monkeypatch.setattr(instance, "_acquire_frame_source", prepare)
    first = instance.send({}, token, descriptor, (b"x",))
    try:
        assert started.wait(timeout=1)
        queued = instance.send({}, token, descriptor, (b"y",))
        clock[0] = instance._timeout_seconds + 1
    finally:
        release.set()
    for future in (first, queued):
        with pytest.raises(NixlWriteNotStarted, match="including queue wait"):
            future.result(timeout=1)
    assert len(agent.register_calls) == len(agent.deregister_calls) == 1
    assert agent.transfer_calls == []
    assert instance._retained_resources == []
    assert instance._failed_peers == set()
    assert instance.diagnostics["queue_wait_seconds"] >= instance._timeout_seconds
    assert instance.send({}, token, descriptor, (b"z",)).result(timeout=1) is None


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_expired_prepared_send_releases_unposted_handle_without_write(runtime, monkeypatch, cleanup_fails):
    instance, agent = runtime
    descriptor = _descriptor("pre-write-timeout", 1)
    agent.metadata_names[b"metadata"] = "peer"
    agent.release_error = cleanup_fails
    initialize = agent.initialize_xfer
    check_deadline = instance._check_send_deadline

    def expire(deadline):
        raise NixlError("send deadline expired")

    def initialize_then_expire(*args, **kwargs):
        handle = initialize(*args, **kwargs)
        monkeypatch.setattr(instance, "_check_send_deadline", expire)
        return handle

    monkeypatch.setattr(agent, "initialize_xfer", initialize_then_expire)
    token = _token("peer", b"metadata", descriptor)
    with pytest.raises(NixlWriteNotStarted, match="deadline expired"):
        instance.send({}, token, descriptor, (b"x",)).result(timeout=1)
    assert agent.transfer_calls == []
    assert ("peer" in instance._failed_peers) == cleanup_fails
    assert bool(instance._retained_resources) == cleanup_fails
    if not cleanup_fails:
        monkeypatch.setattr(instance, "_check_send_deadline", check_deadline)
        monkeypatch.setattr(agent, "initialize_xfer", initialize)
        assert instance.send({}, token, descriptor, (b"x",)).result(timeout=1) is None


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_local_preparation_failure_only_fails_peer_if_cleanup_fails(runtime, monkeypatch, cleanup_fails):
    instance, agent = runtime
    descriptor = _descriptor("prepare-failure", 1, 1)
    agent.metadata_names[b"metadata"] = "peer"
    agent.deregister_error = cleanup_fails
    acquire = instance._acquire_frame_source
    calls = 0

    def prepare(frame):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise MemoryError("source allocation failed")
        return acquire(frame)

    monkeypatch.setattr(instance, "_acquire_frame_source", prepare)
    token = _token("peer", b"metadata", descriptor)
    with pytest.raises(NixlWriteNotStarted, match="source allocation failed"):
        instance.send({}, token, descriptor, (b"x", b"y")).result(timeout=1)
    assert agent.transfer_calls == []
    assert ("peer" in instance._failed_peers) == cleanup_fails
    assert bool(instance._retained_resources) == cleanup_fails
    if not cleanup_fails:
        assert instance.send({}, token, descriptor, (b"x", b"y")).result(timeout=1) is None


def test_metadata_failure_still_fails_peer_before_handle_creation(runtime):
    instance, _ = runtime
    descriptor = _descriptor("bad-metadata", 1)
    token = _token("peer", b"unknown-metadata", descriptor)
    with pytest.raises(NixlWriteNotStarted):
        instance.send({}, token, descriptor, (b"x",)).result(timeout=1)
    with pytest.raises(NixlError, match="has failed"):
        instance.send({}, token, descriptor, (b"x",))


def test_failed_write_retains_resources_and_other_peer_still_works(runtime):
    instance, agent = runtime
    descriptor = _descriptor("failure", 1)
    agent.metadata_names.update({b"bad": "bad-peer", b"good": "good-peer"})
    agent.status_by_remote["bad-peer"] = "ERR"

    with pytest.raises(NixlError, match="status"):
        instance.send({}, _token("bad-peer", b"bad", descriptor), descriptor, (bytearray(b"x"),)).result()

    assert instance._retained_resources
    assert instance.send({}, _token("good-peer", b"good", descriptor), descriptor, (bytearray(b"y"),)).result() is None


def test_failed_future_does_not_retain_handle_or_sources(runtime):
    instance, agent = runtime
    descriptor = _descriptor("failure-traceback", 1)
    agent.metadata_names[b"metadata"] = "peer"
    agent.status_by_remote["peer"] = "ERR"

    future = instance.send({}, _token("peer", b"metadata", descriptor), descriptor, (bytearray(b"x"),))
    error = future.exception(timeout=1)

    assert isinstance(error, NixlError)
    assert not isinstance(error, NixlWriteNotStarted)
    traceback = error.__traceback__
    while traceback is not None and traceback.tb_frame.f_code.co_name != "_send_scatter":
        traceback = traceback.tb_next
    assert traceback is not None
    assert traceback.tb_frame.f_locals["handle"] is None
    assert traceback.tb_frame.f_locals["sources"] == []


def test_close_interrupts_active_polling(runtime):
    instance, agent = runtime
    descriptor = _descriptor("closing", 1)
    agent.metadata_names[b"metadata"] = "peer"
    agent.status_by_remote["peer"] = "PROC"
    future = instance.send({}, _token("peer", b"metadata", descriptor), descriptor, (bytearray(b"x"),))

    deadline = time.monotonic() + 1
    while not agent.transfer_calls and time.monotonic() < deadline:
        time.sleep(0.001)
    started = time.monotonic()
    instance.close()

    assert time.monotonic() - started < 1
    with pytest.raises(NixlError, match="closing"):
        future.result()


def test_close_tears_down_agent_before_retained_owners():
    events: list[str] = []

    class TrackingAgent:
        def __del__(self) -> None:
            events.append("agent")

    class AgentHandle:
        def __init__(self, agent: TrackingAgent, release_lease: Callable[[], None]) -> None:
            self.agent = agent
            self.release_lease = release_lease

        def __del__(self) -> None:
            events.append("handle")
            self.release_lease()

    class TrackingBuffer(bytearray):
        def __init__(self, name: str) -> None:
            super().__init__(b"x")
            self.name = name

        def __del__(self) -> None:
            events.append(self.name)

    instance = object.__new__(NixlRuntime)
    instance._lock = threading.RLock()
    instance._closing = threading.Event()
    instance._closed = False
    instance._peer_senders = {}
    instance._registered_sources = {}
    instance._receives = {}
    instance._idle_receive_buffers = []
    instance._leased_receive_buffers = {}
    instance._remote_metadata = {}
    instance._registered_bytes = 2
    instance._quarantined_bytes = 1

    agent = TrackingAgent()
    source = _FrameSource(TrackingBuffer("source"), None, 0, 1)
    receiver = _RegisteredReceiveBuffer(TrackingBuffer("receiver"), object(), 0)
    leased_receiver = _RegisteredReceiveBuffer(TrackingBuffer("leased_receiver"), object(), 1)
    lease_key = id(leased_receiver)
    handle = AgentHandle(agent, lambda: instance._release_lease(lease_key))
    instance._agent = agent
    instance._retained_resources = [(handle, [source])]
    instance._quarantined_receive_buffers = [receiver]
    instance._leased_receive_buffers[lease_key] = leased_receiver
    del agent, handle, source, receiver, leased_receiver

    instance.close()
    gc.collect()

    assert events.index("handle") < events.index("agent")
    assert events.index("agent") < events.index("source")
    assert events.index("agent") < events.index("receiver")
    assert events.index("agent") < events.index("leased_receiver")


def test_quarantine_never_returns_exposed_receiver_to_pool(runtime):
    instance, _ = runtime
    descriptor = _descriptor("unsafe", 32)
    instance.prepare_receive(descriptor)
    scratch = instance._receives["unsafe"]

    instance.quarantine_receive("unsafe")

    assert instance._idle_receive_buffers == []
    assert instance._quarantined_receive_buffers == [scratch]
    assert instance.diagnostics["quarantined_bytes"] == 32

    safe = _descriptor("safe", 8)
    instance.prepare_receive(safe)
    assert instance.diagnostics["receive_working_set_hwm_count"] == 1
    assert instance.diagnostics["receive_working_set_hwm_bytes"] == 32
    assert instance.diagnostics["registered_bytes"] == 40


def test_get_commit_returns_deferred_response_and_maps_completion():
    transfer = object.__new__(NixlPayloadTransfer)
    descriptor = _descriptor("get", 1)
    transfer._pending_gets = {"get": _PendingGet(descriptor, "manager", (bytearray(b"x"),))}
    send_future: Future[None] = Future()
    transfer.send = lambda *args, **kwargs: send_future
    request = ZMQMessage.create(
        request_type=ZMQRequestType.GET_DATA_COMMIT,
        sender_id="manager",
        body={
            "transfer_id": "get",
            "receiver_endpoint": {"transport": "nixl-ucx", "data": {}},
            "receive_token": {"data": {}},
        },
    )

    response = transfer._handle_get_commit(request, "storage")

    assert isinstance(response, DeferredResponse)
    assert not response.future.done()
    send_future.set_result(None)
    assert response.future.result().request_type == ZMQRequestType.GET_DATA_RESPONSE


@pytest.mark.parametrize("failure", [NixlError("write failed"), NixlWriteNotStarted("not posted"), None])
def test_get_commit_maps_transfer_failure_to_normal_error_response(failure):
    transfer = object.__new__(NixlPayloadTransfer)
    descriptor = _descriptor("failed-get", 1)
    transfer._pending_gets = {"failed-get": _PendingGet(descriptor, "manager", (bytearray(b"x"),))}
    send_future: Future[None] = Future()
    transfer.send = lambda *args, **kwargs: send_future
    request = ZMQMessage.create(
        request_type=ZMQRequestType.GET_DATA_COMMIT,
        sender_id="manager",
        body={
            "transfer_id": "failed-get",
            "receiver_endpoint": {"transport": "nixl-ucx", "data": {}},
            "receive_token": {"data": {}},
        },
    )

    response = transfer._handle_get_commit(request, "storage")
    if failure is None:
        send_future.cancel()
    else:
        send_future.set_exception(failure)

    assert isinstance(response, DeferredResponse)
    assert response.future.result().request_type == ZMQRequestType.PUT_GET_ERROR
    assert response.future.result().body["transfer_id"] == "failed-get"
    assert response.future.result().body["write_not_started"] == (
        failure is None or isinstance(failure, NixlWriteNotStarted)
    )


class _ProtocolSocket:
    def __init__(self, *, fail_commit: bool = False, fail_ready: bool = False) -> None:
        self.fail_commit = fail_commit
        self.fail_ready = fail_ready
        self.last_request: ZMQMessage | None = None

    async def send_multipart(self, frames: Any, **kwargs: Any) -> None:
        request = ZMQMessage.deserialize(frames)
        if self.fail_commit and request.request_type in (
            ZMQRequestType.GET_DATA_COMMIT,
            ZMQRequestType.PUT_DATA_COMMIT,
        ):
            raise RuntimeError("commit send failed")
        self.last_request = request

    async def recv_multipart(self, **kwargs: Any) -> Any:
        assert self.last_request is not None
        request = self.last_request
        if request.request_type == ZMQRequestType.PUT_DATA_PREPARE:
            if self.fail_ready:
                raise RuntimeError("ready receive failed")
            response = ZMQMessage.create(
                request_type=ZMQRequestType.PUT_DATA_READY,
                sender_id="storage",
                body={
                    "descriptor": request.body["descriptor"],
                    "receive_token": {"data": {}},
                },
            )
        elif request.request_type == ZMQRequestType.GET_DATA_PREPARE:
            descriptor = _descriptor(str(request.body["transfer_id"]), 1)
            response = ZMQMessage.create(
                request_type=ZMQRequestType.GET_DATA_READY,
                sender_id="storage",
                body={"descriptor": descriptor.to_dict()},
            )
        elif request.request_type == ZMQRequestType.GET_DATA_COMMIT:
            response = ZMQMessage.create(
                request_type=ZMQRequestType.GET_DATA_RESPONSE,
                sender_id=request.receiver_id,
                body={"transfer_id": request.body["transfer_id"]},
            )
        else:
            raise AssertionError(f"unexpected receive after {request.request_type}")
        return response.serialize()


class _LoopbackSocket:
    def __init__(self, storage: NixlPayloadTransfer):
        self.storage = storage

    async def send_multipart(self, frames, **kwargs):
        self.response = self.storage.handle_request(
            ZMQMessage.deserialize(frames),
            storage_id="storage",
            load_data=lambda *args: {"value": [torch.arange(8)]},
            store_data=lambda *args: pytest.fail("failed PUT must not publish data"),
        )

    async def recv_multipart(self, **kwargs):
        response = self.response
        if isinstance(response, DeferredResponse):
            response = await asyncio.wrap_future(response.future)
        return response.serialize()


@pytest.mark.parametrize("operation", ["put", "get"])
def test_prewrite_failure_releases_receiver_across_protocol_and_runtime(runtime, operation):
    storage = NixlPayloadTransfer(receive_buffer_cache_bytes=0)
    client = NixlPayloadTransfer(peer_infos={"storage": storage.bootstrap_info()}, receive_buffer_cache_bytes=0)
    sender = client if operation == "put" else storage
    sender._runtime._timeout_seconds = 0

    async def exercise():
        for _ in range(2):
            kwargs = {"data": {"value": [1]}, "data_parser": None} if operation == "put" else {"fields": ["value"]}
            with pytest.raises(RuntimeError, match="timed out"):
                await getattr(client, operation)(
                    control_socket=_LoopbackSocket(storage),
                    sender_id="manager",
                    target_id="storage",
                    global_indexes=[1],
                    **kwargs,
                )
            assert storage._pending_puts == storage._pending_gets == {}
            for transfer in (storage, client):
                assert transfer._runtime._receives == {}
                assert transfer.diagnostics["registered_bytes"] == 0
                assert transfer.diagnostics["quarantined_bytes"] == 0
            assert client._failed_get_targets == set()
        assert sender._runtime._agent.transfer_calls == []
        assert sender._runtime._failed_peers == set()

    try:
        asyncio.run(exercise())
    finally:
        client.close()
        storage.close()


@pytest.mark.parametrize("failing_call", ["np.frombuffer", "_frame_views"])
def test_put_commit_releases_receive_when_view_creation_fails(runtime, monkeypatch, failing_call):
    storage = NixlPayloadTransfer(receive_buffer_cache_bytes=0)
    descriptor = _descriptor("failed-view", 16)
    prepare = ZMQMessage.create(
        request_type=ZMQRequestType.PUT_DATA_PREPARE,
        sender_id="manager",
        body={"descriptor": descriptor.to_dict(), "global_indexes": [1]},
    )
    commit = ZMQMessage.create(
        request_type=ZMQRequestType.PUT_DATA_COMMIT,
        sender_id="other-manager",
        body={"transfer_id": descriptor.transfer_id},
    )

    def fail_owner(*args, **kwargs):
        raise MemoryError("view allocation failed")

    try:
        assert storage._handle_put_prepare(prepare, "storage").request_type == ZMQRequestType.PUT_DATA_READY
        store_data = lambda *args: pytest.fail("failed PUT must not publish data")
        assert storage._handle_put_commit(commit, "storage", store_data).request_type == ZMQRequestType.PUT_GET_ERROR
        assert descriptor.transfer_id in storage._pending_puts
        assert descriptor.transfer_id in storage._runtime._receives

        monkeypatch.setattr(f"transfer_queue.storage.payload_transfer.nixl_ucx_runtime.{failing_call}", fail_owner)
        commit = ZMQMessage.create(
            request_type=ZMQRequestType.PUT_DATA_COMMIT,
            sender_id="manager",
            body={"transfer_id": descriptor.transfer_id},
        )
        response = storage._handle_put_commit(commit, "storage", store_data)

        assert response.request_type == ZMQRequestType.PUT_GET_ERROR
        assert "view allocation failed" in response.body["message"]
        assert storage._pending_puts == storage._runtime._receives == {}
        assert storage.diagnostics["registered_bytes"] == 0
    finally:
        storage.close()


@pytest.mark.parametrize("error_type", [NixlError, NixlWriteNotStarted])
def test_put_only_cancels_failed_send_when_write_never_started(error_type):
    transfer = object.__new__(NixlPayloadTransfer)
    failed_send: Future[None] = Future()
    failed_send.set_exception(error_type("write failed"))
    transfer._peer_endpoint = lambda target_id: TransferEndpoint("nixl-ucx", {})
    transfer.send = lambda *args, **kwargs: failed_send
    cancellations: list[Any] = []

    async def cancel(*args: Any) -> None:
        cancellations.append(args)

    transfer._cancel = cancel
    with pytest.raises(NixlError, match="write failed"):
        asyncio.run(
            transfer.put(
                control_socket=_ProtocolSocket(),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                data={"value": [1]},
                data_parser=None,
            )
        )

    assert bool(cancellations) == (error_type is NixlWriteNotStarted)


def test_put_cancels_prepared_receiver_before_send_is_attempted():
    transfer = object.__new__(NixlPayloadTransfer)

    def missing_endpoint(target_id: str) -> TransferEndpoint:
        raise RuntimeError("endpoint unavailable")

    transfer._peer_endpoint = missing_endpoint
    cancellations: list[Any] = []

    async def cancel(*args: Any) -> None:
        cancellations.append(args)

    transfer._cancel = cancel
    with pytest.raises(RuntimeError, match="endpoint unavailable"):
        asyncio.run(
            transfer.put(
                control_socket=_ProtocolSocket(),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                data={"value": [1]},
                data_parser=None,
            )
        )

    assert cancellations[0][2] == ZMQRequestType.PUT_DATA_CANCEL


@pytest.mark.parametrize("send_state", ["done", "queued", "running_done", "running_unposted", "running_failed"])
def test_put_cancellation_at_write_completion_only_cancels_safe_receives(send_state):
    transfer = object.__new__(NixlPayloadTransfer)
    transfer._peer_endpoint = lambda target_id: TransferEndpoint("nixl-ucx", {})
    send_future = Future()
    if send_state == "done":
        send_future.set_result(None)
    elif send_state.startswith("running"):
        send_future.set_running_or_notify_cancel()
    cancellations = []
    control_socket = _ProtocolSocket()

    def send(*args):
        loop = asyncio.get_running_loop()
        loop.call_soon(asyncio.current_task().cancel)
        if send_state == "running_done":
            loop.call_later(0.01, send_future.set_result, None)
        elif send_state == "running_unposted":
            loop.call_later(0.01, send_future.set_exception, NixlWriteNotStarted("not posted"))
        elif send_state == "running_failed":
            loop.call_later(0.01, send_future.set_exception, NixlError("uncertain write"))
        return send_future

    async def cancel(*args):
        assert send_future.done()
        cancellations.append(args)

    transfer.send = send
    transfer._cancel = cancel
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            transfer.put(
                control_socket=control_socket,
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                data={"value": [1]},
                data_parser=None,
            )
        )
    assert send_future.done()
    assert bool(cancellations) == (send_state != "running_failed")
    if cancellations:
        assert cancellations[0][-1] is control_socket
    assert control_socket.last_request.request_type == ZMQRequestType.PUT_DATA_PREPARE


def test_put_does_not_cancel_after_commit_attempt():
    transfer = object.__new__(NixlPayloadTransfer)
    transfer._peer_endpoint = lambda target_id: TransferEndpoint("nixl-ucx", {})
    completed = Future()
    completed.set_result(None)
    transfer.send = lambda *args: completed

    async def cancel(*args):
        pytest.fail("COMMIT may already have published the data")

    transfer._cancel = cancel
    with pytest.raises(RuntimeError, match="commit send failed"):
        asyncio.run(
            transfer.put(
                control_socket=_ProtocolSocket(fail_commit=True),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                data={"value": [1]},
                data_parser=None,
            )
        )


@pytest.mark.parametrize("operation", ["PUT", "GET"])
def test_cancel_drains_late_ready_and_matches_ack_on_original_socket(operation):
    transfer = object.__new__(NixlPayloadTransfer)
    replies = [
        ZMQMessage.create(request_type=ZMQRequestType[f"{operation}_DATA_READY"], sender_id="storage", body={}),
        ZMQMessage.create(
            request_type=ZMQRequestType[f"{operation}_DATA_RESPONSE"],
            sender_id="storage",
            body={"transfer_id": "another-transfer"},
        ),
        ZMQMessage.create(
            request_type=ZMQRequestType[f"{operation}_DATA_RESPONSE"],
            sender_id="storage",
            body={"transfer_id": "cancelled-transfer"},
        ),
    ]

    class CancelSocket(_ProtocolSocket):
        async def recv_multipart(self, **kwargs):
            return replies.pop(0).serialize()

    control_socket = CancelSocket()
    asyncio.run(
        transfer._cancel(
            "manager", "storage", ZMQRequestType[f"{operation}_DATA_CANCEL"], "cancelled-transfer", control_socket
        )
    )
    assert replies == []
    assert control_socket.last_request.request_type == ZMQRequestType[f"{operation}_DATA_CANCEL"]


def test_put_cancels_when_send_fails_before_submission():
    transfer = object.__new__(NixlPayloadTransfer)
    transfer._peer_endpoint = lambda target_id: TransferEndpoint("nixl-ucx", {})

    def failed_send(*args: Any, **kwargs: Any) -> Future[None]:
        raise NixlError("metadata invalid")

    transfer.send = failed_send
    cancellations: list[Any] = []

    async def cancel(*args: Any) -> None:
        cancellations.append(args)

    transfer._cancel = cancel
    with pytest.raises(NixlError, match="metadata invalid"):
        asyncio.run(
            transfer.put(
                control_socket=_ProtocolSocket(),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                data={"value": [1]},
                data_parser=None,
            )
        )

    assert cancellations[0][2] == ZMQRequestType.PUT_DATA_CANCEL


def test_put_attempts_cancel_when_ready_is_lost():
    transfer = object.__new__(NixlPayloadTransfer)
    cancellations: list[Any] = []

    async def cancel(*args: Any) -> None:
        cancellations.append(args)

    transfer._cancel = cancel
    with pytest.raises(RuntimeError, match="ready receive failed"):
        asyncio.run(
            transfer.put(
                control_socket=_ProtocolSocket(fail_ready=True),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                data={"value": [1]},
                data_parser=None,
            )
        )

    assert cancellations[0][2] == ZMQRequestType.PUT_DATA_CANCEL


def test_get_cancels_remote_source_when_local_receive_prepare_fails():
    transfer = object.__new__(NixlPayloadTransfer)
    transfer._failed_get_targets = set()

    def failed_prepare(descriptor: PayloadDescriptor) -> ReceiveToken:
        raise NixlError("registration failed")

    transfer.prepare_receive = failed_prepare
    cancellations: list[Any] = []

    async def cancel(*args: Any) -> None:
        cancellations.append(args)

    transfer._cancel = cancel
    with pytest.raises(NixlError, match="registration failed"):
        asyncio.run(
            transfer.get(
                control_socket=_ProtocolSocket(),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                fields=["value"],
            )
        )

    assert cancellations[0][2] == ZMQRequestType.GET_DATA_CANCEL


def test_get_commit_attempt_failure_quarantines_without_cancel():
    transfer = object.__new__(NixlPayloadTransfer)
    transfer._failed_get_targets = set()
    transfer.prepare_receive = lambda descriptor: ReceiveToken({"agent_name": "manager"})
    quarantined: list[str] = []
    cancellations: list[Any] = []
    transfer.quarantine_receive = quarantined.append
    transfer.cancel_receive = lambda transfer_id: pytest.fail("exposed receiver was cancelled")

    async def cancel(*args: Any) -> None:
        cancellations.append(args)

    transfer._cancel = cancel
    with pytest.raises(RuntimeError, match="commit send failed"):
        asyncio.run(
            transfer.get(
                control_socket=_ProtocolSocket(fail_commit=True),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                fields=["value"],
            )
        )

    assert len(quarantined) == 1
    assert cancellations == []
    assert transfer._failed_get_targets == {"storage"}

    retry_socket = _ProtocolSocket()
    with pytest.raises(NixlError, match="GET session.*has failed"):
        asyncio.run(
            transfer.get(
                control_socket=retry_socket,
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                fields=["value"],
            )
        )
    assert retry_socket.last_request is None


@pytest.mark.parametrize("failure_stage", ["receive", "decode"])
def test_get_local_failure_after_ack_does_not_quarantine_or_fail_target(monkeypatch, failure_stage):
    transfer = object.__new__(NixlPayloadTransfer)
    transfer._failed_get_targets = {"unrelated-target"}
    transfer.prepare_receive = lambda descriptor: ReceiveToken({"agent_name": "manager"})
    cancellations = []
    transfer.cancel_receive = cancellations.append
    transfer.quarantine_receive = lambda transfer_id: pytest.fail("WRITE was already confirmed complete")
    received = Future()
    if failure_stage == "receive":
        received.set_exception(NixlError("local receive failed"))
    else:
        received.set_result((memoryview(b"x"),))

        def decode_failure(frames):
            raise RuntimeError("local decode failed")

        monkeypatch.setattr("transfer_queue.storage.payload_transfer.nixl.decode", decode_failure)
    transfer.receive = lambda descriptor: received
    with pytest.raises(RuntimeError, match=f"local {failure_stage} failed"):
        asyncio.run(
            transfer.get(
                control_socket=_ProtocolSocket(),
                sender_id="manager",
                target_id="storage",
                global_indexes=[1],
                fields=["value"],
            )
        )
    assert len(cancellations) == 1
    assert transfer._failed_get_targets == {"unrelated-target"}


class _CapturingSocket:
    def __init__(self) -> None:
        self.messages: list[Any] = []

    def send_multipart(self, message: Any, **kwargs: Any) -> None:
        self.messages.append(message)


def test_deferred_response_copies_identity_and_worker_sends_once():
    future: Future[ZMQMessage] = Future()
    response = DeferredResponse(future)
    completions: SimpleQueue[tuple[bytes, Future[ZMQMessage]]] = SimpleQueue()
    reader, writer = socket.socketpair()
    reader.settimeout(1)
    poller = zmq.Poller()
    poller.register(reader.fileno(), zmq.POLLIN)
    shutdown = threading.Event()
    identity = bytearray(b"client-a")
    worker = _CapturingSocket()
    timing_finished: list[bool] = []
    measurement = ExitStack()
    measurement.callback(timing_finished.append, True)
    measurements = {future: measurement}
    try:
        _queue_deferred_response(identity, response, completions, writer, shutdown)
        identity[:] = b"client-b"
        future.set_result(
            ZMQMessage.create(request_type=ZMQRequestType.GET_DATA_RESPONSE, sender_id="storage", body={})
        )
        assert dict(poller.poll(1000))[reader.fileno()] == zmq.POLLIN
        assert reader.recv(1) == b"\0"
        assert timing_finished == []
        _drain_deferred_responses(completions, worker, "storage", measurements)
        _drain_deferred_responses(completions, worker, "storage", measurements)
    finally:
        reader.close()
        writer.close()

    assert len(worker.messages) == 1
    assert worker.messages[0][0] == b"client-a"
    assert timing_finished == [True]
    assert measurements == {}
