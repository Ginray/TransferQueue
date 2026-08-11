"""Build the optional UCX extension when a local UCX installation is found."""

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
    mode = os.environ.get("TQ_BUILD_UCX", "auto").lower()
    if mode in {"0", "false", "off"}:
        return None
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
    if mode in {"1", "true", "on"}:
        raise RuntimeError("TQ_BUILD_UCX requested a native build, but no UCX installation was found")
    return None


def ucx_extension() -> list[Extension]:
    installation = find_ucx()
    if installation is None:
        return []
    import pybind11

    include_dir, libdir = installation
    return [
        Extension(
            "transfer_queue._ucx",
            ["transfer_queue/native/ucx/ucx_bindings.cpp"],
            include_dirs=[pybind11.get_include(), str(include_dir)],
            library_dirs=[str(libdir)],
            libraries=["ucp", "uct", "ucs", "ucm"],
            language="c++",
            extra_compile_args=["-std=c++17"],
        )
    ]


setup(ext_modules=ucx_extension())
