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

"""Payload transfer contract, UCX runtime and lifecycle tests."""

from __future__ import annotations

import os
from pathlib import Path
from threading import Event

import pytest
from omegaconf import OmegaConf

from transfer_queue.storage.payload_transfer import (
    PayloadDescriptor,
    PayloadTransferError,
    ReceiveToken,
    TransferEndpoint,
    create_payload_transfer,
)
from transfer_queue.storage.payload_transfer.nixl import NixlPayloadTransfer
from transfer_queue.storage.payload_transfer.ucx import UcxPayloadTransfer
from transfer_queue.storage.payload_transfer.ucx_discovery import (
    UcxDeviceSelection,
    _parse_ucx_info_devices,
    discover_ucx_device,
    gid_matches_ip,
)
from transfer_queue.storage.payload_transfer.ucx_runtime import (
    UcxError,
    UcxRuntime,
    UcxTransfer,
    _UcxOwnerThread,
    address_digest,
    transfer_tag,
)


class _FakeRequest:
    def __init__(self, pending_polls: int, result: str = "done"):
        self.pending_polls = pending_polls
        self.result = result
        self.cancelled = False
        self.started = Event()

    def test(self):
        self.started.set()
        if self.pending_polls:
            self.pending_polls -= 1
            return None
        return self.result

    def cancel(self) -> None:
        self.cancelled = True

    def start_cancel(self) -> None:
        self.cancelled = True

    def test_cancel(self):
        return True if self.cancelled else None


def test_owner_progresses_multiple_requests_without_blocking_each_other():
    owner = _UcxOwnerThread(lambda: None, lambda: None)
    try:
        first = _FakeRequest(pending_polls=2, result="first")
        second = _FakeRequest(pending_polls=0, result="second")

        first_future = owner.submit_request(lambda: first, lambda request: request.test(), None, 1)
        second_future = owner.submit_request(lambda: second, lambda request: request.test(), None, 1)

        assert second_future.result(timeout=1) == "second"
        assert first_future.result(timeout=1) == "first"
    finally:
        owner.stop()


def test_progress_failure_is_reported_to_active_request():
    failed = False

    def progress():
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("worker failed")

    owner = _UcxOwnerThread(lambda: None, progress)
    try:
        request = _FakeRequest(pending_polls=1_000_000)
        future = owner.submit_request(lambda: request, lambda pending: pending.test(), None, 10)

        with pytest.raises(UcxError, match="UCX progress failed"):
            future.result(timeout=1)
        assert request.cancelled
    finally:
        owner.stop()


def test_owner_cancels_active_request_before_shutdown():
    owner = _UcxOwnerThread(lambda: None, lambda: None)
    try:
        request = _FakeRequest(pending_polls=1_000_000)
        future = owner.submit_request(lambda: request, lambda pending: pending.test(), None, 10)
        assert request.started.wait(timeout=1)

        owner.cancel_active_requests()

        assert request.cancelled
        with pytest.raises(UcxError, match="canceled during shutdown"):
            future.result(timeout=1)
    finally:
        owner.stop()


def test_cancelled_future_does_not_block_later_owner_tasks():
    owner = _UcxOwnerThread(lambda: None, lambda: None)
    try:
        request = _FakeRequest(pending_polls=1_000_000)
        future = owner.submit_request(lambda: request, lambda pending: pending.test(), None, 10)
        assert request.started.wait(timeout=1)

        assert future.cancel()
        assert owner.submit(lambda: "still-responsive").result(timeout=1) == "still-responsive"
        assert request.cancelled
    finally:
        owner.stop()


def test_ucx_transfer_rejects_inconsistent_control_metadata():
    transfer_id = "descriptor-validation"
    tag = transfer_tag(transfer_id)

    UcxTransfer(transfer_id, tag, 3).validate()
    invalid = (
        UcxTransfer(transfer_id, tag + 1, 3),
        UcxTransfer(transfer_id, tag, -1),
    )
    for descriptor in invalid:
        with pytest.raises(UcxError):
            descriptor.validate()


def test_payload_descriptor_is_always_complete_when_deserialized():
    descriptor = PayloadDescriptor.from_dict({"transfer_id": "payload", "payload_bytes": 3})
    assert descriptor.payload_bytes == 3

    with pytest.raises(PayloadTransferError, match="negative"):
        PayloadDescriptor.from_dict({"transfer_id": "payload", "payload_bytes": -1})


def test_payload_descriptor_preserves_frame_layout():
    descriptor = PayloadDescriptor("framed", 8 + 16 * 2 + 5, (2, 3))
    descriptor.validate()
    restored = PayloadDescriptor.from_dict(descriptor.to_dict())
    assert restored == descriptor

    with pytest.raises(PayloadTransferError, match="packed payload length"):
        PayloadDescriptor("framed", 5, (2, 3)).validate()


def test_ucx_adapter_sends_encoded_frames_without_packing():
    class Runtime:
        address = b"a" * 32

        def send(self, address, transfer, frames, digest):
            self.sent = address, transfer, frames, digest
            return "frame-future"

    adapter = UcxPayloadTransfer.__new__(UcxPayloadTransfer)
    adapter._runtime = Runtime()
    frames = (b"ab", b"cde")
    descriptor = PayloadDescriptor("framed", 8 + 16 * 2 + 5, (2, 3))
    endpoint = TransferEndpoint("ucx", {"address": b"b" * 32})

    assert adapter.send(endpoint, ReceiveToken(data={}), descriptor, frames) == "frame-future"
    assert adapter._runtime.sent[2] == frames
    assert adapter._runtime.sent[1].frame_sizes == (2, 3)


def test_ucx_runtime_direct_send_receive_roundtrip():
    mailbox = {}

    class Receive:
        def __init__(self, tag, target):
            self.tag = tag
            self.target = target

        def wait(self, _timeout):
            payload = mailbox[self.tag]
            assert len(payload) == len(self.target)
            self.target[:] = payload
            return self.target

    class Worker:
        def post_receive_into(self, tag, target):
            return Receive(tag, target)

    class Endpoint:
        def post_send(self, tag, payload):
            mailbox[tag] = bytes(payload)
            return _FakeRequest(0)

    plane = UcxRuntime.__new__(UcxRuntime)
    plane._timeout_seconds = 1
    plane._receives = {}
    plane._receive_buffers = {}
    plane._reusable_receive_buffer = None
    plane._worker = Worker()
    plane._closed = False
    plane._call = lambda operation: operation()
    peer_address = b"p" * 32
    plane._endpoints = {peer_address: Endpoint()}
    transfer_id = "direct-roundtrip"
    descriptor = UcxTransfer(transfer_id, transfer_tag(transfer_id), 8 + 16 + 10, (10,))

    plane.prepare_receive(descriptor)
    send = plane._post_send(peer_address, None, descriptor, (b"abcdefghij",))
    assert send.test() is True

    payload = plane.finish_receive(descriptor)
    assert bytes(payload[24:]) == b"abcdefghij"


def test_peer_address_digest_is_checked_before_ucx_submission():
    address = b"a" * 32
    UcxRuntime._validate_peer_address(address, address_digest(address))

    with pytest.raises(UcxError, match="digest"):
        UcxRuntime._validate_peer_address(address, address_digest(b"b" * 32))

    with pytest.raises(UcxError, match="length"):
        UcxRuntime._validate_peer_address(b"short")


def test_receive_uses_actual_length_reported_by_binding():
    descriptor = UcxTransfer("short-receive", transfer_tag("short-receive"), 8)

    with pytest.raises(UcxError, match="received length mismatch"):
        UcxRuntime._received_payload(descriptor, memoryview(b"short"))


def test_roce_device_is_discovered_from_local_ip_and_gid(tmp_path):
    port = tmp_path / "rdma0" / "ports" / "1"
    for relative, value in (
        ("gid_attrs/ndevs/3", "eth1\n"),
        ("gid_attrs/types/3", "RoCE v2\n"),
        ("gids/3", "::ffff:192.0.2.10\n"),
    ):
        path = port / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)

    selection = discover_ucx_device(
        "192.0.2.10",
        infiniband_root=tmp_path,
        interface_addresses={"eth1": {"192.0.2.10"}},
    )

    assert selection == UcxDeviceSelection("rdma0", 1, "eth1", 3)
    assert selection.net_devices == "rdma0:1,eth1"
    assert gid_matches_ip("::ffff:192.0.2.10", "192.0.2.10")


def test_unique_roce_device_is_discovered_when_control_ip_is_separate(tmp_path):
    port = tmp_path / "rdma0" / "ports" / "1"
    for relative, value in (
        ("gid_attrs/ndevs/3", "eth1\n"),
        ("gid_attrs/types/3", "RoCE v2\n"),
        ("gids/3", "::ffff:192.0.2.10\n"),
    ):
        path = port / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)

    selection = discover_ucx_device(
        "198.51.100.10",
        infiniband_root=tmp_path,
        interface_addresses={"eth1": {"192.0.2.10"}},
    )

    assert selection == UcxDeviceSelection("rdma0", 1, "eth1", 3)


def test_ambiguous_roce_devices_are_not_guessed(tmp_path):
    for device, netdev, address in (
        ("rdma0", "eth1", "192.0.2.10"),
        ("rdma1", "eth2", "192.0.2.11"),
    ):
        port = tmp_path / device / "ports" / "1"
        for relative, value in (
            ("gid_attrs/ndevs/3", f"{netdev}\n"),
            ("gid_attrs/types/3", "RoCE v2\n"),
            ("gids/3", f"::ffff:{address}\n"),
        ):
            path = port / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value)

    selection = discover_ucx_device(
        "198.51.100.10",
        infiniband_root=tmp_path,
        interface_addresses={"eth1": {"192.0.2.10"}, "eth2": {"192.0.2.11"}},
    )

    assert selection == UcxDeviceSelection()


def test_ucx_config_is_derived_from_local_discovery():
    # The actual transport selection is runtime capability based.  Keep this
    # test focused on the node-local device/GID settings.
    selection = UcxDeviceSelection("rdma0", 1, "eth1", 3)
    assert selection.ucx_config == {
        "NET_DEVICES": "rdma0:1,eth1",
        "IB_GID_INDEX": "3",
        "IB_ADDR_TYPE": "ib_global",
    }


def test_ucx_info_devices_are_parsed_without_vendor_assumptions():
    output = """
#      Transport: rc_verbs
#         Device: hca0:1
#      Transport: tcp
#         Device: eth9
#      Transport: self
#         Device: memory
"""

    assert _parse_ucx_info_devices(output) == {
        ("rc_verbs", "hca0:1"),
        ("tcp", "eth9"),
        ("self", "memory"),
    }


def test_ucx_config_selects_runtime_supported_transports(monkeypatch):
    from transfer_queue.storage.payload_transfer import ucx_discovery

    monkeypatch.setattr(
        ucx_discovery,
        "_discover_ucx_transports",
        lambda: frozenset(
            {
                ("rc_verbs", "hca0:1"),
                ("tcp", "eth9"),
                ("sysv", "memory"),
                ("self", "memory"),
            }
        ),
    )

    selection = UcxDeviceSelection("hca0", 1, "eth9", 3)

    assert selection.ucx_config["TLS"] == "rc_verbs,tcp,sm,self"


def test_ucx_info_discovery_supports_lib64_installations(monkeypatch, tmp_path):
    from transfer_queue.storage.payload_transfer import ucx_discovery

    prefix = tmp_path / "ucx"
    executable = prefix / "bin" / "ucx_info"
    executable.parent.mkdir(parents=True)
    executable.touch()
    (prefix / "lib64").mkdir()
    captured = {}

    class Result:
        stdout = "#      Transport: tcp\n#         Device: eth9\n"

    def run(_args, *, env, **_kwargs):
        captured.update(env)
        return Result()

    monkeypatch.setattr(ucx_discovery, "_find_ucx_info", lambda: executable)
    monkeypatch.setattr(ucx_discovery.subprocess, "run", run)
    monkeypatch.setenv("LD_LIBRARY_PATH", "/existing/lib")
    ucx_discovery._discover_ucx_transports.cache_clear()

    try:
        assert ucx_discovery._discover_ucx_transports() == frozenset({("tcp", "eth9")})
        assert captured["LD_LIBRARY_PATH"] == f"{prefix / 'lib64'}:/existing/lib"
    finally:
        ucx_discovery._discover_ucx_transports.cache_clear()


def test_ucx_info_discovery_uses_tq_ucx_home(monkeypatch, tmp_path):
    from transfer_queue.storage.payload_transfer import ucx_discovery

    prefix = tmp_path / "ucx"
    executable = prefix / "bin" / "ucx_info"
    executable.parent.mkdir(parents=True)
    executable.touch()
    executable.chmod(0o755)
    monkeypatch.setenv("TQ_UCX_HOME", str(prefix))
    monkeypatch.delenv("TQ_UCX_INFO", raising=False)

    assert ucx_discovery._find_ucx_info() == executable


def test_explicit_ucx_tls_overrides_runtime_selection(monkeypatch):
    monkeypatch.setenv("UCX_TLS", "tcp,self")
    monkeypatch.setenv("UCX_NET_DEVICES", "custom_hca:2,custom_eth")
    monkeypatch.setenv("UCX_IB_GID_INDEX", "7")
    monkeypatch.setenv("UCX_IB_ADDR_TYPE", "ib_global")

    selection = UcxDeviceSelection("rdma0", 1, "eth1", 3)
    assert selection.ucx_config == {
        "TLS": "tcp,self",
        "NET_DEVICES": "custom_hca:2,custom_eth",
        "IB_GID_INDEX": "7",
        "IB_ADDR_TYPE": "ib_global",
    }


def test_ucx_payload_transfer_fails_when_ucx_is_unavailable(monkeypatch):
    monkeypatch.setenv("UCX_TLS", "rc_verbs")
    monkeypatch.setattr(
        "transfer_queue.storage.payload_transfer.ucx_runtime.discover_ucx_device",
        lambda _ip: UcxDeviceSelection("rdma0", 1, "eth0", 3),
    )

    class UnavailableRuntime:
        def __init__(self, **_kwargs):
            raise RuntimeError("UCX unavailable")

    monkeypatch.setattr("transfer_queue.storage.payload_transfer.ucx_runtime.UcxRuntime", UnavailableRuntime)

    with pytest.raises(UcxError, match="UCX unavailable"):
        create_payload_transfer("ucx", local_ip="192.0.2.1")


def test_ucx_payload_transfer_requires_an_rdma_device(monkeypatch):
    monkeypatch.delenv("UCX_NET_DEVICES", raising=False)
    monkeypatch.setattr(
        "transfer_queue.storage.payload_transfer.ucx_runtime.discover_ucx_device",
        lambda _ip: UcxDeviceSelection(),
    )

    constructed = False

    class UnexpectedRuntime:
        def __init__(self, **_kwargs):
            nonlocal constructed
            constructed = True

    monkeypatch.setattr("transfer_queue.storage.payload_transfer.ucx_runtime.UcxRuntime", UnexpectedRuntime)

    with pytest.raises(UcxError, match="no RoCE-v2 device"):
        create_payload_transfer("ucx", local_ip="192.0.2.1")
    assert not constructed


def test_ucx_payload_transfer_rejects_tcp_only_tls(monkeypatch):
    monkeypatch.setenv("UCX_TLS", "tcp,sm,self")
    monkeypatch.setattr(
        "transfer_queue.storage.payload_transfer.ucx_runtime.discover_ucx_device",
        lambda _ip: UcxDeviceSelection("rdma0", 1, "eth0", 3),
    )

    with pytest.raises(UcxError, match="no reliable-connection RDMA transport"):
        create_payload_transfer("ucx", local_ip="192.0.2.1")


def test_payload_transfer_rejects_unknown_implementation():
    with pytest.raises(ValueError, match="expected 'zmq', 'ucx' or 'nixl-ucx'"):
        create_payload_transfer("hixl")


def test_nixl_adapter_preserves_payload_transfer_contract():
    class Runtime:
        agent_name = "receiver"

        def endpoint_metadata(self):
            return b"endpoint-metadata"

        def prepare_receive(self, descriptor):
            self.prepared = descriptor
            return {
                "agent_name": self.agent_name,
                "agent_metadata": b"receive-metadata",
                "frame_remote_descs": b"frame-remote-descs",
                "payload_bytes": descriptor.payload_bytes,
            }

        def send(self, endpoint, token, descriptor, frames):
            self.sent_frames = endpoint, token, descriptor, frames
            return "frame-future"

        def receive(self, descriptor):
            self.received = descriptor
            return "receive-future"

        def cancel_receive(self, transfer_id):
            self.cancelled = transfer_id

        pending_receive_count = 1

        def close(self):
            self.closed = True

    adapter = NixlPayloadTransfer.__new__(NixlPayloadTransfer)
    adapter._runtime = Runtime()
    descriptor = PayloadDescriptor("nixl-contract", 8 + 16 + 3, (3,))

    endpoint = adapter.endpoint()
    token = adapter.prepare_receive(descriptor)
    assert endpoint.transport == "nixl-ucx"
    assert token.data["agent_name"] == "receiver"
    assert adapter.send(endpoint, token, descriptor, (b"abc",)) == "frame-future"
    assert adapter._runtime.sent_frames[3] == (b"abc",)
    assert adapter.receive(descriptor) == "receive-future"
    assert adapter.pending_receive_count == 1


def test_nixl_reuses_ucx_discovery_for_missing_environment(monkeypatch):
    from transfer_queue.storage.payload_transfer import nixl_runtime

    class Selection:
        rdma_device = "hns_0"
        ucx_config = {
            "TLS": "rc_verbs,tcp,sm,self",
            "NET_DEVICES": "hns_0:1,enp189s0f0",
            "IB_GID_INDEX": "3",
            "IB_ADDR_TYPE": "ib_global",
        }

    for name in ("UCX_TLS", "UCX_NET_DEVICES", "UCX_IB_GID_INDEX", "UCX_IB_ADDR_TYPE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(nixl_runtime, "discover_ucx_device", lambda local_ip: Selection())

    assert nixl_runtime._configure_ucx_environment("178.123.4.4") == Selection.ucx_config
    assert os.environ["UCX_TLS"] == "rc_verbs,tcp,sm,self"
    assert os.environ["UCX_NET_DEVICES"] == "hns_0:1,enp189s0f0"
    assert os.environ["UCX_IB_GID_INDEX"] == "3"
    assert os.environ["UCX_IB_ADDR_TYPE"] == "ib_global"


def test_public_config_defaults_to_zmq_payload_transfer():
    config = OmegaConf.load(Path(__file__).parents[1] / "transfer_queue/config.yaml")

    assert config.backend.SimpleStorage.payload_transfer == "zmq"
    assert "payload_transport" not in config.backend.SimpleStorage
    assert "data_plane" not in config.backend.SimpleStorage
