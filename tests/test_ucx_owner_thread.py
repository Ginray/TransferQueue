# Copyright 2026 The TransferQueue Team
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Deterministic lifecycle tests for the thread-affine UCX owner loop."""

from __future__ import annotations

from pathlib import Path
from threading import Event

import pytest
from omegaconf import OmegaConf

from transfer_queue.storage.data_plane import (
    DEFAULT_TRANSFER_TIMEOUT_SECONDS,
    DataPlaneError,
    PayloadDescriptor,
    UcxDataPlane,
    _UcxOwnerThread,
    address_digest,
    create_data_plane,
    transfer_tag,
)
from transfer_queue.storage.ucx_discovery import (
    UcxDeviceSelection,
    _parse_ucx_info_devices,
    discover_ucx_device,
    gid_matches_ip,
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


def test_owner_allows_unbounded_request():
    owner = _UcxOwnerThread(lambda: None, lambda: None)
    try:
        request = _FakeRequest(pending_polls=1, result="unbounded")
        future = owner.submit_request(lambda: request, lambda pending: pending.test(), None, None)

        assert future.result(timeout=1) == "unbounded"
        assert DEFAULT_TRANSFER_TIMEOUT_SECONDS is None
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
        with pytest.raises(DataPlaneError, match="canceled during shutdown"):
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


def test_payload_descriptor_rejects_inconsistent_control_metadata():
    transfer_id = "descriptor-validation"
    tag = transfer_tag(transfer_id)

    PayloadDescriptor(transfer_id, tag, 3, 1).validate()
    large = PayloadDescriptor(transfer_id, tag, 64 * 1024 * 1024 * 1024 + 1, 1)
    large.validate()
    assert PayloadDescriptor.from_dict(large.to_dict()) == large

    invalid = (
        PayloadDescriptor(transfer_id, tag + 1, 3, 1),
        PayloadDescriptor(transfer_id, tag, -1, 1),
        PayloadDescriptor(transfer_id, tag, 3, 0),
    )
    for descriptor in invalid:
        with pytest.raises(DataPlaneError):
            descriptor.validate()


def test_data_plane_direct_send_receive_roundtrip():
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

    class Native:
        def post_receive(self, tag, size):
            return Receive(tag, bytearray(size))

    class Endpoint:
        def post_send(self, tag, payload):
            mailbox[tag] = bytes(payload)
            return _FakeRequest(0)

    plane = UcxDataPlane.__new__(UcxDataPlane)
    plane._timeout_seconds = 1
    plane._receives = {}
    plane._native = Native()
    plane._closed = False
    plane._call = lambda operation: operation()
    peer_address = b"p" * 32
    plane._endpoints = {peer_address: Endpoint()}
    transfer_id = "direct-roundtrip"
    descriptor = PayloadDescriptor(transfer_id, transfer_tag(transfer_id), 10, 1)

    plane.prepare_receive(descriptor)
    send = plane._post_send(peer_address, None, descriptor, b"abcdefghij")
    assert send.test() == "done"

    assert bytes(plane.finish_receive(descriptor)) == b"abcdefghij"


def test_peer_address_digest_is_checked_before_native_submission():
    address = b"a" * 32
    UcxDataPlane._validate_peer_address(address, address_digest(address))

    with pytest.raises(DataPlaneError, match="digest"):
        UcxDataPlane._validate_peer_address(address, address_digest(b"b" * 32))


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
    from transfer_queue.storage import ucx_discovery

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


def test_payload_transport_falls_back_when_ucx_is_unavailable(monkeypatch):
    monkeypatch.setattr(
        "transfer_queue.storage.data_plane.discover_ucx_device",
        lambda _ip: UcxDeviceSelection(),
    )

    class UnavailableTransport:
        def __init__(self, **_kwargs):
            raise RuntimeError("UCX unavailable")

    monkeypatch.setattr("transfer_queue.storage.data_plane.UcxDataPlane", UnavailableTransport)

    assert create_data_plane(False, local_ip="192.0.2.1") is None
    assert create_data_plane(True, local_ip="192.0.2.1") is None


def test_payload_transport_does_not_masquerade_ucx_tcp_as_rdma(monkeypatch):
    monkeypatch.delenv("UCX_NET_DEVICES", raising=False)
    monkeypatch.setattr(
        "transfer_queue.storage.data_plane.discover_ucx_device",
        lambda _ip: UcxDeviceSelection(),
    )

    constructed = False

    class UnexpectedTransport:
        def __init__(self, **_kwargs):
            nonlocal constructed
            constructed = True

    monkeypatch.setattr("transfer_queue.storage.data_plane.UcxDataPlane", UnexpectedTransport)

    assert create_data_plane(True, local_ip="192.0.2.1") is None
    assert not constructed


def test_public_config_exposes_only_the_payload_transport_switch():
    config = OmegaConf.load(Path(__file__).parents[1] / "transfer_queue/config.yaml")

    assert OmegaConf.to_container(config.backend.SimpleStorage.payload_transport) == {"enabled": False}
    assert "data_plane" not in config.backend.SimpleStorage
