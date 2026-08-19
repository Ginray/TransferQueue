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

"""Node-local UCX device selection for the UCX payload transfer."""

from __future__ import annotations

import importlib
import ipaddress
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import psutil

_RC_TRANSPORTS = ("rc_mlx5", "rc_verbs", "rc_v", "rc_x")
_LOCAL_TRANSPORTS = {"sm", "sysv", "posix", "xpmem", "cma", "knem"}
_TRANSPORT_LINE = re.compile(r"^\s*#\s+Transport:\s+(\S+)")
_DEVICE_LINE = re.compile(r"^\s*#\s+Device:\s+(\S+)")


@dataclass(frozen=True)
class UcxDeviceSelection:
    """RDMA device, port and GID selected for one local IP."""

    rdma_device: str | None = None
    port: int | None = None
    netdev: str | None = None
    gid_index: int | None = None

    @property
    def net_devices(self) -> str | None:
        if self.rdma_device is None or self.port is None:
            return None
        rdma = f"{self.rdma_device}:{self.port}"
        return f"{rdma},{self.netdev}" if self.netdev else rdma

    @property
    def ucx_config(self) -> dict[str, str]:
        """Return UCP context settings for this node-local selection."""
        config: dict[str, str] = {}
        if self.rdma_device:
            # Do not infer a transport from the vendor or device name.  Select
            # only from transports advertised by this UCX runtime.  An
            # explicit process setting remains authoritative.
            tls = _select_host_ucx_tls(self.rdma_device, self.port or 0, self.netdev)
            if tls:
                config["TLS"] = tls
        if self.net_devices:
            config["NET_DEVICES"] = os.environ.get("UCX_NET_DEVICES", self.net_devices)
        if self.gid_index is not None:
            config["IB_GID_INDEX"] = os.environ.get("UCX_IB_GID_INDEX", str(self.gid_index))
            config["IB_ADDR_TYPE"] = os.environ.get("UCX_IB_ADDR_TYPE", "ib_global")
        elif os.environ.get("UCX_IB_GID_INDEX") is not None:
            config["IB_GID_INDEX"] = os.environ["UCX_IB_GID_INDEX"]
            config["IB_ADDR_TYPE"] = os.environ.get("UCX_IB_ADDR_TYPE", "ib_global")
        return config


def discover_ucx_device(
    local_ip: str | None,
    infiniband_root: Path = Path("/sys/class/infiniband"),
    interface_addresses: dict[str, set[str]] | None = None,
) -> UcxDeviceSelection:
    """Find the RoCE-v2 GID that carries ``local_ip``.

    An exact local-IP match wins.  If the Ray/control IP is on another network,
    a single unambiguous RoCE-v2 candidate is accepted; multiple candidates are
    left unresolved rather than guessed.
    """
    if not infiniband_root.is_dir():
        return UcxDeviceSelection()
    try:
        normalized_ip = str(ipaddress.ip_address(local_ip.split("%", 1)[0])) if local_ip else None
    except ValueError:
        normalized_ip = None
    interface_addresses = interface_addresses or _interface_addresses()
    candidates: list[UcxDeviceSelection] = []

    for rdma_path in sorted(infiniband_root.iterdir()):
        ports_path = rdma_path / "ports"
        if not ports_path.is_dir():
            continue
        for port_path in sorted(ports_path.iterdir()):
            try:
                port = int(port_path.name)
            except ValueError:
                continue
            ndevs_path = port_path / "gid_attrs" / "ndevs"
            gids_path = port_path / "gids"
            types_path = port_path / "gid_attrs" / "types"
            if not ndevs_path.is_dir() or not gids_path.is_dir():
                continue
            for ndev_path in sorted(ndevs_path.iterdir(), key=lambda path: int(path.name)):
                netdev = _read_text(ndev_path)
                gid = _read_text(gids_path / ndev_path.name)
                gid_type = _read_text(types_path / ndev_path.name)
                if not netdev or (gid_type and "v2" not in gid_type.lower()):
                    continue
                assigned_addresses = interface_addresses.get(netdev, set())
                if not any(gid_matches_ip(gid, address) for address in assigned_addresses):
                    continue
                selection = UcxDeviceSelection(rdma_path.name, port, netdev, int(ndev_path.name))
                if normalized_ip and gid_matches_ip(gid, normalized_ip):
                    return selection
                candidates.append(selection)
    unique_candidates = list(dict.fromkeys(candidates))
    return unique_candidates[0] if len(unique_candidates) == 1 else UcxDeviceSelection()


def _select_host_ucx_tls(rdma_device: str, port: int, netdev: str | None) -> str | None:
    explicit = os.environ.get("UCX_TLS")
    if explicit is not None:
        return explicit

    capabilities = _discover_ucx_transports()
    rdma_address = f"{rdma_device}:{port}"
    rc_transport = next(
        (
            transport
            for transport in _RC_TRANSPORTS
            if (transport, rdma_address) in capabilities or (transport, rdma_device) in capabilities
        ),
        None,
    )
    if rc_transport is None:
        return None

    selected = [rc_transport]
    if ("tcp", netdev) in capabilities:
        selected.append("tcp")
    elif ("ud_verbs", rdma_address) in capabilities:
        # RC may use UD only as its auxiliary wireup transport.  Do not add
        # UD to the data lanes unless UCX advertises no TCP alternative.
        selected.append("ud_verbs:aux")

    if capabilities.intersection({(name, "memory") for name in _LOCAL_TRANSPORTS}):
        selected.append("sm")
    if ("self", "memory") in capabilities:
        selected.append("self")
    return ",".join(selected)


@lru_cache(maxsize=1)
def _discover_ucx_transports() -> frozenset[tuple[str, str]]:
    executable = _find_ucx_info()
    if executable is None:
        return frozenset()

    environment = os.environ.copy()
    for name in ("UCX_TLS", "UCX_NET_DEVICES", "UCX_IB_GID_INDEX", "UCX_IB_ADDR_TYPE"):
        environment.pop(name, None)
    prefix = executable.parent.parent
    library_paths = [path for path in (prefix / "lib", prefix / "lib64") if path.is_dir()]
    if library_paths:
        existing = environment.get("LD_LIBRARY_PATH")
        paths = [str(path) for path in library_paths]
        if existing:
            paths.append(existing)
        environment["LD_LIBRARY_PATH"] = ":".join(paths)
    try:
        result = subprocess.run(
            [str(executable), "-d"],
            env=environment,
            capture_output=True,
            check=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return frozenset()
    return frozenset(_parse_ucx_info_devices(result.stdout))


def _find_ucx_info() -> Path | None:
    configured = os.environ.get("TQ_UCX_INFO")
    if configured:
        path = Path(configured)
        return path if path.is_file() and os.access(path, os.X_OK) else None

    # Prefer the UCX runtime loaded by the native extension.  A system
    # ``ucx_info`` in PATH may describe a different UCX installation.
    try:
        importlib.import_module("transfer_queue._ucx")

        for line in Path("/proc/self/maps").read_text().splitlines():
            fields = line.split()
            if not fields or "libucp.so" not in fields[-1]:
                continue
            candidate = Path(fields[-1]).parent.parent / "bin" / "ucx_info"
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return candidate
    except (ImportError, OSError):
        pass

    path = shutil.which("ucx_info")
    if path:
        return Path(path)
    return None


def _parse_ucx_info_devices(output: str) -> set[tuple[str, str]]:
    capabilities: set[tuple[str, str]] = set()
    transport: str | None = None
    for line in output.splitlines():
        transport_match = _TRANSPORT_LINE.match(line)
        if transport_match:
            transport = transport_match.group(1)
            continue
        device_match = _DEVICE_LINE.match(line)
        if transport and device_match:
            capabilities.add((transport, device_match.group(1)))
    return capabilities


def gid_matches_ip(gid: str | None, local_ip: str) -> bool:
    if not gid:
        return False
    try:
        gid_address = ipaddress.ip_address(gid)
        local_address = ipaddress.ip_address(local_ip)
    except ValueError:
        return False
    return gid_address == local_address or (
        isinstance(gid_address, ipaddress.IPv6Address) and gid_address.ipv4_mapped == local_address
    )


def _interface_addresses() -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for interface, addresses in psutil.net_if_addrs().items():
        normalized = set()
        for address in addresses:
            try:
                normalized.add(str(ipaddress.ip_address(address.address.split("%", 1)[0])))
            except ValueError:
                continue
        result[interface] = normalized
    return result


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except (OSError, UnicodeError):
        return None
