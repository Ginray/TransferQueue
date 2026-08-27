# SimpleStorage NIXL-UCX RDMA Payload 传输

> 最后更新：2026/08/24

## 概述

TransferQueue 支持通过 NIXL 的 UCX backend，在跨节点 `SimpleStorage` 的 PUT/GET 操作中传输不小于 `128 KiB` 的 Host payload。控制请求和较小的 payload 仍然使用 ZMQ。

启用方式：

```yaml
backend:
  SimpleStorage:
    payload_transfer: nixl-ucx
```

本文介绍 NIXL-UCX 的依赖准备、UCX 检查、TQ 安装、配置和端到端验证。

## 1. 确认 RDMA 设备

在运行 TQ 或 Ray worker 的每个节点执行：

```bash
# 查看 RDMA 设备名称，例如 hns_0 或 mlx5_0
ls /sys/class/infiniband
# 查看 RDMA 设备和端口状态，端口通常应为 ACTIVE
rdma dev show
# 查看网卡和 IP，确认节点间通信使用的网卡
ip -br addr
```

`/sys/class/infiniband` 不应为空，RDMA 端口通常应为 `ACTIVE`。如果看不到设备，检查 RDMA 驱动和容器设备映射。

## 2. 准备 NIXL 和 UCX

NIXL-UCX 不是独立的 RDMA backend，NIXL 必须使用带 UCX plugin 的构建版本。当前 A2 验证环境使用：

- NIXL `1.4.0`，source commit `90cce46`；
- 启用 UCX plugin，当前 H2H 路径不需要 CUDA plugin；
- UCX `/opt/tq-ucx/1.22.0-mt`，构建时已启用 UCX multi-thread；
- Python `3.11.15`，Conda 环境为 `test`。

如果使用其他 NIXL 版本，请使用该版本对应的安装或源码构建方式，并确认构建结果包含 UCX plugin。TQ 不修改 NIXL 源码。

### 使用已有 NIXL 构建

如果环境已经提供带 UCX plugin 的 NIXL wheel 或安装目录，直接安装到运行 TQ 的 Python 环境，并确保 NIXL 和 UCX 动态库对 Driver、StorageUnit 和 Ray worker 可见。

### 从源码构建

以下命令使用已验证的 NIXL source commit，并将 NIXL 安装到当前 Conda 环境。NIXL 的构建依赖和选项可能随版本变化；切换版本时以对应版本的构建说明为准。

```bash
# 指定 UCX 安装目录
export TQ_UCX_HOME=/opt/tq-ucx/1.22.0-mt
# 确认当前使用的是目标 Conda 环境
test -n "${CONDA_PREFIX:?please activate the target Conda environment}"
# 安装 NIXL 的 Meson/Python 构建工具
python -m pip install meson ninja pybind11 tomlkit
# 下载当前验证使用的 NIXL 源码
git clone https://github.com/ai-dynamo/nixl.git /tmp/nixl-ucx
cd /tmp/nixl-ucx
# 固定到当前验证记录中的源码版本
git checkout 90cce46
# 让 NIXL 的构建系统找到非系统目录中的 UCX
export PKG_CONFIG_PATH="${TQ_UCX_HOME}/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
# 配置 Release 构建，并指定 UCX 安装目录
meson setup build \
  -Ducx_path="${TQ_UCX_HOME}" \
  -Dprefix="${CONDA_PREFIX}" \
  -Dbuildtype=release
# 编译 NIXL
ninja -C build
# 安装 NIXL Python binding、库和 UCX plugin
ninja -C build install
```

安装后必须执行第 3 节的 NIXL backend 检查。UCX multi-thread 在构建时通过
`configure-release-mt` 启用，不需要额外的运行时环境变量。只有检查到 `UCX`
backend，才能启用 `payload_transfer: nixl-ucx`。

## 3. 检查 NIXL 和 UCX

以当前验证环境为例，设置 UCX 工具和动态库路径：

```bash
# 指定 UCX 安装目录
export TQ_UCX_HOME=/opt/tq-ucx/1.22.0-mt
# 优先使用该目录中的 UCX 工具
export PATH="${TQ_UCX_HOME}/bin:${PATH}"
# 让 Python、NIXL 和 Ray worker 找到 UCX 动态库
export LD_LIBRARY_PATH="${TQ_UCX_HOME}/lib64:${TQ_UCX_HOME}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
```

检查 UCX 版本、设备和 NIXL UCX backend：

```bash
# 查看 UCX 版本
ucx_info -v
# 查看当前 UCX 支持的设备和 transport
ucx_info -d
# 确认 NIXL Python binding 和 UCX backend 可用
python - <<'PY'
from nixl import nixl_agent, nixl_agent_config

config = nixl_agent_config(
    enable_prog_thread=True,
    enable_listen_thread=True,
    listen_port=0,
    backends=["UCX"],
)
agent = nixl_agent("tq-nixl-check", config)
assert "UCX" in agent.backends
print("NIXL UCX backend is available")
PY
```

`ucx_info -d` 应列出当前设备支持的 `rc_*` transport。NIXL 运行时使用 TQ 的 UCX 设备发现逻辑；设置 `TQ_UCX_HOME` 后，会优先使用该目录下的 `ucx_info`。

也可以使用 `ucx_perftest` 单独检查 UCX 跨节点连通性。该检查只验证 UCX，不代表 SimpleStorage 已经完成端到端验证。

节点 A：

```bash
# 在节点 A 启动 UCX 测试服务端
ucx_perftest -c 0
```

节点 B：

```bash
# 从节点 B 连接节点 A，执行 1 MiB tag 带宽测试
ucx_perftest <node_a_ip> -t tag_bw -s 1048576 -n 20 -c 1
```

## 4. 安装 TQ

NIXL-UCX 路径使用 TQ 的 Python 适配层，不要求构建 TQ 的原生 `_ucx` 扩展。当前配置下，`TQ_UCX_HOME` 提供 UCX 检查工具，NIXL 提供实际的 UCX 数据传输 backend。

在 TQ 源码目录执行：

```bash
# 安装当前 TQ 源码及其 Python 依赖
python -m pip install -e .
```

检查 TQ、NIXL Python binding 是否能在同一个环境中导入：

```bash
# 验证 TQ 和 NIXL 都来自当前 Python 环境
python - <<'PY'
import transfer_queue
from nixl import nixl_agent

print("TransferQueue:", transfer_queue.__file__)
print("NIXL: available")
PY
```

Driver 和 Ray worker 必须使用兼容的 TQ、NIXL 和 UCX 运行时。Ray worker 也必须能找到本地 NIXL 和 UCX 动态库。

## 5. 启用 SimpleStorage NIXL-UCX 传输

配置 `SimpleStorage`：

```yaml
backend:
  storage_backend: SimpleStorage
  SimpleStorage:
    payload_transfer: nixl-ucx
```

通常不需要手动指定网卡名称、端口或 GID。TQ 会发现本机 RDMA 设备、RoCE GID、网卡和可用的 `rc_*` transport，并将 UCX 配置提供给 NIXL。

如果节点有多张网卡、自动发现失败，或需要固定通信链路，可以在启动 Ray 和 TQ 前，在所有参与节点设置：

```bash
# 指定 RDMA transport 和辅助 transport
export UCX_TLS=<rc_transport>,tcp,sm,self
# 指定 RDMA 设备、端口和对应的网卡
export UCX_NET_DEVICES=<rdma_device>:<port>,<netdev>
# 指定 RoCE-v2 GID 索引
export UCX_IB_GID_INDEX=<gid_index>
# 使用 RoCE-v2 地址类型
export UCX_IB_ADDR_TYPE=ib_global
```

| 变量 | 含义 |
| --- | --- |
| `UCX_TLS` | 根据 `ucx_info -d` 选择设备支持的 `rc_*` transport，并按环境加入 `tcp,sm,self`。 |
| `UCX_NET_DEVICES` | RDMA 设备和端口，以及对应的 Ethernet 网卡，例如 `hns_0:1,enp189s0f0`。 |
| `UCX_IB_GID_INDEX` | 与节点通信 IP 对应的 RoCE-v2 GID 索引。 |
| `UCX_IB_ADDR_TYPE` | RoCE-v2 使用 `ib_global`。 |

当前 A2 验证使用：

```bash
# A2-26/A2-27 验证过的 UCX 配置示例
export UCX_TLS=rc_verbs,tcp,sm,self
export UCX_NET_DEVICES=hns_0:1,enp189s0f0
export UCX_IB_GID_INDEX=3
export UCX_IB_ADDR_TYPE=ib_global
```

其中 `rc_verbs/hns_0:1` 承载 RMA payload，TCP 用于 UCX wireup/keepalive，不是 payload fallback。设备名、端口和 GID 索引需要根据节点实际环境替换。

## 6. 验证 SimpleStorage NIXL-UCX 传输

启动 Ray 和应用前，可以打开 UCX 日志：

```bash
# 输出 UCX 的 transport 和 endpoint 信息
export UCX_LOG_LEVEL=info
```

1. 启动后，StorageUnit 日志中应能看到类似信息：

   ```text
   SimpleStorage payload transfer selected: nixl-ucx ... tls=rc_verbs,tcp,sm,self
   ```

2. 使用编码后不小于 `128 KiB` 的 Host payload，完成跨节点 PUT、GET 和 CLEAR，并检查：

   - PUT 后能够 GET 到相同内容；
   - 小于 `128 KiB` 的 payload 仍走 ZMQ；
   - CLEAR 后数据不可再次读取。

3. 在 StorageUnit 或 Ray worker 日志中查找 NIXL/UCX 的 endpoint 信息，例如：

   ```text
   inter-node cfg#1 rma(rc_verbs/hns_0:1) ... ka(tcp/enp189s0f0)
   ```

   `rma(rc_*/...)` 表示 payload 使用 RDMA lane；`ka(tcp/...)` 表示 TCP 用于连接建立或 keepalive。

## 常见问题

### `No module named nixl`

当前 Python 环境没有安装 NIXL。安装或构建带 UCX plugin 的 NIXL，并确保 Driver 和 Ray worker 使用同一个兼容环境。

### `NIXL UCX backend is not available`

当前 NIXL 构建没有包含 UCX plugin，或者 NIXL 的 native library/plugin 没有被正确加载。检查 NIXL 构建选项和 `LD_LIBRARY_PATH`。

### `libucp.so` 或 NIXL plugin 找不到

检查 `TQ_UCX_HOME`，并将 UCX 的 `lib`/`lib64` 和 NIXL 动态库目录加入 `LD_LIBRARY_PATH`。Ray worker 也必须继承这些环境变量。

### `NIXL_ERR_BACKEND` 或 `no connect to iface`

检查 `UCX_TLS` 是否包含当前设备支持的 `rc_*` transport，以及是否包含当前 UCX/网络环境需要的辅助 transport。当前 A2 配置需要 `tcp` 参与 UCX wireup；只设置 `rc_verbs,sm,self` 会在 worker 初始化阶段失败。

### NIXL payload 没有走 RDMA

检查 `ucx_info -d`、`UCX_NET_DEVICES` 和 `UCX_IB_GID_INDEX`。同时查看 UCX endpoint 日志中的 `rma(rc_*/...)`，不要只根据 `SimpleStorage payload transfer selected` 日志判断实际 payload lane。
