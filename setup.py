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

"""Build the optional UCX extension when explicitly requested."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from setuptools import Extension, setup


def _valid_ucx_installation(include_dir: Path, libdir: Path) -> tuple[Path, Path] | None:
    if (include_dir / "ucp/api/ucp.h").is_file() and any(libdir.glob("libucp.so*")):
        return include_dir, libdir
    return None


def _pkg_config_ucx() -> tuple[Path, Path] | None:
    try:
        values = [
            subprocess.run(
                ["pkg-config", f"--variable={variable}", "ucx"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            for variable in ("includedir", "libdir")
        ]
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    if not all(values):
        return None
    return _valid_ucx_installation(Path(values[0]), Path(values[1]))


def _prefix_ucx(prefix: Path) -> tuple[Path, Path] | None:
    include_dir = prefix / "include"
    libdirs = [prefix / "lib", prefix / "lib64", *sorted((prefix / "lib").glob("*-linux-gnu"))]
    for libdir in libdirs:
        installation = _valid_ucx_installation(include_dir, libdir)
        if installation is not None:
            return installation
    return None


def find_ucx() -> tuple[Path, Path] | None:
    mode = os.environ.get("TQ_BUILD_UCX", "0").lower()
    if mode in {"0", "false", "off"}:
        return None
    if mode not in {"1", "true", "on"}:
        raise RuntimeError("TQ_BUILD_UCX must be one of: 0, false, off, 1, true, on")
    configured = os.environ.get("TQ_UCX_HOME")
    if configured:
        installation = _prefix_ucx(Path(configured))
        if installation is not None:
            return installation
    installation = _pkg_config_ucx()
    if installation is not None:
        return installation
    for prefix in (Path("/usr/local"), Path("/usr")):
        installation = _prefix_ucx(prefix)
        if installation is not None:
            return installation
    raise RuntimeError("TQ_BUILD_UCX requested a UCX build, but no UCX installation was found")


def ucx_extension() -> list[Extension]:
    installation = find_ucx()
    if installation is None:
        return []
    import pybind11

    include_dir, libdir = installation
    return [
        Extension(
            "transfer_queue._ucx",
            ["transfer_queue/csrc/ucx/ucx_bindings.cpp"],
            include_dirs=[pybind11.get_include(), str(include_dir)],
            library_dirs=[str(libdir)],
            libraries=["ucp", "uct", "ucs", "ucm"],
            language="c++",
            extra_compile_args=["-std=c++17"],
        )
    ]


setup(ext_modules=ucx_extension())
