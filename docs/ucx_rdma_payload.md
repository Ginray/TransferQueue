# SimpleStorage UCX RDMA Payload Transfer

> Last updated: 08/19/2026

## Overview

TransferQueue supports UCX RDMA for Host payloads of at least `128 KiB` in cross-node
`SimpleStorage` PUT/GET operations. Control requests and smaller payloads continue to use ZMQ.
Enable it explicitly with:

```yaml
backend:
  SimpleStorage:
    payload_transfer: ucx
```

This document covers UCX setup, the TQ build, configuration, and RDMA-path verification.

## 1. Check RDMA devices

Run the following on every node running TQ or a Ray worker:

```bash
# RDMA device directory; normally lists names such as mlx5_0
ls /sys/class/infiniband
# RDMA device and port state; the port should normally be ACTIVE
rdma dev show
# Network interfaces and IP addresses; identify the interface used between nodes
ip -br addr
```

The device directory should not be empty and the port should be `ACTIVE`. If no device is
visible, check the RDMA driver or container device mapping.

## 2. Prepare UCX (v1.22.0 example)

Use the same UCX version on all participating nodes. Check the operating system and architecture,
then select a matching package from the [UCX v1.22.0 release page](https://github.com/openucx/ucx/releases/tag/v1.22.0):

```bash
uname -m
. /etc/os-release && printf '%s %s\n' "$ID" "$VERSION_ID"
```

### Use a prebuilt package

The following example targets AArch64/CentOS with MOFED 5. Replace `UCX_PACKAGE` with a package
matching the local operating system, architecture, and RDMA software stack.

```bash
UCX_VERSION=1.22.0
TQ_UCX_HOME=/opt/tq-ucx/${UCX_VERSION}-mt
UCX_PACKAGE=ucx-1.22.0-centos8-mofed5-cuda11-aarch64.tar.bz2
UCX_RPM_DIR=/tmp/tq-ucx-rpms

# Download and extract the package
curl -fL --retry 2 \
  -o "/tmp/${UCX_PACKAGE}" \
  "https://github.com/openucx/ucx/releases/download/v${UCX_VERSION}/${UCX_PACKAGE}"
mkdir -p "${UCX_RPM_DIR}"
tar -xjf "/tmp/${UCX_PACKAGE}" -C "${UCX_RPM_DIR}"

# Install the UCX libraries, verbs/RDMA-CM support, and development headers
sudo rpm -Uvh --replacepkgs --prefix="${TQ_UCX_HOME}" \
  "${UCX_RPM_DIR}"/ucx-${UCX_VERSION}-*.rpm \
  "${UCX_RPM_DIR}"/ucx-devel-${UCX_VERSION}-*.rpm \
  "${UCX_RPM_DIR}"/ucx-ib-${UCX_VERSION}-*.rpm \
  "${UCX_RPM_DIR}"/ucx-rdmacm-${UCX_VERSION}-*.rpm
```

Mellanox `mlx5` adapters also need the matching UCX plugin:

```bash
sudo rpm -Uvh --replacepkgs --prefix="${TQ_UCX_HOME}" \
  "${UCX_RPM_DIR}"/ucx-ib-mlx5-${UCX_VERSION}-*.rpm
```

### Build from source

If this UCX installation is used with NIXL's native progress thread, build the
multi-thread variant shown below.

Install the build dependencies:

```bash
sudo dnf install gcc gcc-c++ make autoconf automake libtool \
  libibverbs-devel librdmacm-devel rdma-core-devel
```

Build and install the v1.22.0 tag:

```bash
git clone --depth 1 --branch v1.22.0 \
  https://github.com/openucx/ucx.git /tmp/ucx-v1.22.0
cd /tmp/ucx-v1.22.0

./autogen.sh
# 构建启用多线程支持的 UCX
./contrib/configure-release-mt \
  --prefix=/opt/tq-ucx/1.22.0-mt \
  --with-verbs \
  --with-rdmacm
make -j"$(nproc)"
sudo make install
```

## 3. Check UCX

Set the UCX tools, libraries, and TQ build paths:

```bash
export TQ_UCX_HOME=/opt/tq-ucx/1.22.0-mt
export PATH="${TQ_UCX_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${TQ_UCX_HOME}/lib64:${TQ_UCX_HOME}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
```

Check the version and available transports:

```bash
ucx_info -v
ucx_info -d
```

`ucx_info -v` should report v1.22.0 and include `--enable-mt` in the configure options.
There is no separate runtime switch for UCX multi-thread support. `ucx_info -d` should list
an `rc_*` transport for the RDMA device. TCP or another auxiliary transport depends on the
UCX and network environment.

You can also test cross-node UCX connectivity with `ucx_perftest`. On node A:

```bash
ucx_perftest -c 0
```

On node B:

```bash
ucx_perftest <node_a_ip> -t tag_bw -s 1048576 -n 20 -c 1
```

## 4. Build the TQ UCX extension

Run the following in the TQ source directory:

```bash
python -m pip install build
TQ_BUILD_UCX=1 TQ_UCX_HOME="${TQ_UCX_HOME}" python -m build --wheel
python -m pip install --force-reinstall --no-deps dist/*.whl
```

Check that the current Python environment loads the UCX extension:

```bash
python -c 'import transfer_queue._ucx as ucx; print(ucx.__file__)'
```

Processes participating in SimpleStorage must use the same TQ wheel and UCX libraries.

## 5. Enable SimpleStorage RDMA transfer

Set the following configuration:

```yaml
backend:
  storage_backend: SimpleStorage
  SimpleStorage:
    payload_transfer: ucx
```

Normally, `UCX_TLS`, `UCX_NET_DEVICES`, and the GID do not need to be set. TQ discovers the
RDMA device, RoCE-v2 GID, network interface, and an available `rc_*` transport, then selects
auxiliary transports according to the environment.

### Advanced option: specify UCX network settings

For multi-NIC nodes, failed automatic discovery, or a fixed communication interface, set the
following before starting Ray and TQ on every participating node:

```bash
export UCX_TLS=<rc_transport>,tcp,sm,self
export UCX_NET_DEVICES=<rdma_device>:<port>,<netdev>
export UCX_IB_GID_INDEX=<gid_index>
export UCX_IB_ADDR_TYPE=ib_global
```

| Variable | Value |
| --- | --- |
| `UCX_TLS` | An `rc_*` transport supported by the device according to `ucx_info -d`, such as `rc_mlx5`, `rc_verbs`, or `rc_x`; add `tcp,sm,self` as needed. |
| `UCX_NET_DEVICES` | The RDMA device and port plus the corresponding Ethernet interface, for example `mlx5_0:1,ens5f0`. |
| `UCX_IB_GID_INDEX` | The RoCE-v2 GID index associated with the node communication IP. |
| `UCX_IB_ADDR_TYPE` | `ib_global` for RoCE-v2. |

`<netdev>` must be the interface carrying the node communication IP shown by `ip -br addr`.
Confirm the `<gid_index>` against sysfs:

```bash
# Print the Ethernet interface, GID type, and GID at the selected index
cat /sys/class/infiniband/<rdma_device>/ports/<port>/gid_attrs/ndevs/<gid_index>
cat /sys/class/infiniband/<rdma_device>/ports/<port>/gid_attrs/types/<gid_index>
cat /sys/class/infiniband/<rdma_device>/ports/<port>/gids/<gid_index>
```

Device names, ports, and GID indices may differ between nodes.

## 6. Verify SimpleStorage RDMA transfer

Set UCX logging before starting Ray and the application on every participating node:

```bash
export UCX_LOG_LEVEL=info
```

1. At startup, each StorageUnit should log a line similar to:

   ```text
   SimpleStorage payload transfer selected: ucx ... tls=<rc_transport>,...
   ```

2. Use a payload whose encoded size is at least `128 KiB` and complete a cross-node PUT, remote
   GET, and CLEAR. Verify the GET content and confirm that the data is gone after CLEAR.

3. Search the StorageUnit or Ray logs for `ep_cfg` and `tag(` endpoint/lane information. For example:

   ```text
   UCX INFO ep_cfg[1]: tag(rc_mlx5/mlx5_0:1) ...
   ```

   `tag(rc_*/...)` indicates that the payload uses an RDMA lane.

## Common issues

### `transfer_queue._ucx` cannot be imported

The installed wheel does not contain the UCX extension. Rebuild with `TQ_BUILD_UCX=1` and use
the same Python environment on Ray workers.

### `libucp.so` cannot be found

Check `TQ_UCX_HOME` and add `${TQ_UCX_HOME}/lib` or `lib64` to `LD_LIBRARY_PATH`.

### `no RoCE-v2 device and GID match local IP`

Check that the node communication IP and the RoCE interface are on the same network plane, and
inspect `/sys/class/infiniband` and the network configuration.

### `no auxiliary transport` or `Destination is unreachable`

Confirm that `ucx_info -d` lists an `rc_*` transport supported by the device. If UCX settings must
be specified manually, use a transport supported by the current device; do not hard-code `rc_verbs`.

### The UCX payload lane uses TCP

Check the RDMA device and `ucx_info -d`, and confirm that `UCX_TLS` includes an `rc_*` transport
supported by the device.
