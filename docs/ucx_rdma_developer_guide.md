# TQ UCX Host RDMA 开发者指南

本文面向需要在开发机或集群上构建、启用和验证 TQ UCX Host RDMA 的开发者。
当前实现只覆盖 **SimpleStorage 的 Host payload transfer**：ZMQ 仍负责控制协议和小
payload，UCX UCP Tagged Send/Receive 负责达到阈值的大块 Host buffer。

本文不是 UCX 通用调优手册，也不覆盖 Mooncake、openYuanrong 或 HIXL 的独立传输配置。

## 1. 当前能力和边界

```text
SimpleStorage KV/control protocol  ── ZMQ（始终保留）
                                      │
large contiguous Host payload      ── UCX Tagged + RC data lane
                                      └─ TCP auxiliary/wireup（需要时）
```

- 默认配置仍为 `payload_transfer: zmq`，不需要安装 UCX。
- `payload_transfer: ucx` 只改变 SimpleStorage 的大 payload 数据面，不替换存储后端、
  路由、序列化或 ZMQ 控制面。
- 当前 UCX payload 路径要求 Host memory、可靠连接 RDMA transport 和 RoCE-v2 GID。
- 小于 `128 KiB` 的 payload 仍走 inline/ZMQ；具体阈值由当前 SimpleStorage payload
  transfer 实现定义，不应当作 UCX 的通用阈值。
- GPU/NPU 直连、GDR、RMA、自动分块不是当前 TQ UCX payload 的能力。
- Mooncake、openYuanrong 和 RayStore 继续使用各自 backend 的 transport，不要因为安装
  UCX 就替这些 backend 设置 TQ UCX payload。

权威设计说明见 [SimpleStorage Payload Transfer RFC](rfcs/simple_storage_payload_transport_guide.md)。

## 2. 节点准入

每个运行 Controller、SimpleStorageUnit 或 TQ client 的节点都需要满足：

1. Linux、可用的 `rdma-core`/libibverbs 和 RoCE/InfiniBand NIC；
2. 所有参与节点能够通过同一网络平面互通；
3. 每个节点的 Python 环境包含 TQ、PyTorch（如业务 payload 使用 Torch）、pyzmq、Ray
   和构建 native extension 所需的 `pybind11`；
4. UCX runtime、UCX native extension 和 Python 环境在每个 Ray worker 节点可见；
5. `memlock`、容器 capability 和防火墙策略允许 RDMA memory registration 与对应的
   auxiliary TCP 连接。

先检查底层设备，不要先把失败归因到 TQ：

```bash
ibv_devices
ibv_devinfo
ip -br addr
```

多网卡机器不应根据网卡名称猜测配置。TQ 会遍历本机 RDMA sysfs，使用本机控制 IP 对应
的 RoCE-v2 GID 选择 RDMA device、port、netdev 和 GID index；多个候选无法唯一确定时会
拒绝初始化，而不是随机选一张卡。

## 3. 安装官方 UCX

不要使用开发中的 UCX 分支或为某台机器维护私有 UCX patch。固定一个官方 release，并在
所有节点使用同一个版本。当前验证版本是官方 [UCX v1.22.0](https://github.com/openucx/ucx/releases/tag/v1.22.0)。

### 3.1 官方二进制/RPM 包

从 release 页面选择与操作系统、架构和 verbs 栈匹配的 asset。下面以 AArch64/CentOS 兼容
RPM 包为例；asset 名称需要按目标平台替换：

```bash
UCX_VERSION=1.22.0
UCX_PREFIX=/opt/tq-ucx/${UCX_VERSION}-official
UCX_ASSET=ucx-1.22.0-centos8-mofed5-cuda11-aarch64.tar.bz2

curl -fL --retry 2 \
  -o "/tmp/${UCX_ASSET}" \
  "https://github.com/openucx/ucx/releases/download/v${UCX_VERSION}/${UCX_ASSET}"

mkdir -p /tmp/ucx-rpms
tar -xjf "/tmp/${UCX_ASSET}" -C /tmp/ucx-rpms

rpm -Uvh --replacepkgs --nodeps --prefix="${UCX_PREFIX}" \
  /tmp/ucx-rpms/ucx-${UCX_VERSION}-*.rpm \
  /tmp/ucx-rpms/ucx-ib-${UCX_VERSION}-*.rpm \
  /tmp/ucx-rpms/ucx-rdmacm-${UCX_VERSION}-*.rpm \
  /tmp/ucx-rpms/ucx-devel-${UCX_VERSION}-*.rpm
```

Host payload 只需要 UCX core、verbs/RDMA-CM 和 devel headers。CUDA/ROCm/GDR package
只有在对应 device-memory backend 已经单独准入时才安装，不要为当前 Host payload 默认引入。

### 3.2 官方源码构建

当 release asset 与目标发行版不匹配时，使用同一个官方 tag 构建，不要切换到带有私有
patch 的分支：

```bash
git clone --depth 1 --branch v1.22.0 https://github.com/openucx/ucx.git /tmp/ucx-v1.22.0
cd /tmp/ucx-v1.22.0
./autogen.sh
./contrib/configure-release \
  --prefix=/opt/tq-ucx/1.22.0-official \
  --with-verbs \
  --with-rdmacm \
  --without-cuda \
  --without-rocm
make -j"$(nproc)"
sudo make install
```

### 3.3 验证安装

下面的环境变量只指向当前进程使用的 UCX，不应写死到 TQ 源码：

```bash
export TQ_UCX_HOME=/opt/tq-ucx/1.22.0-official
export PATH="${TQ_UCX_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${TQ_UCX_HOME}/lib64:${TQ_UCX_HOME}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

ucx_info -v
ucx_info -d
```

确认：

- `ucx_info -v` 的 version/revision 是预期的官方 release；
- `ucx_info -d` 同时能看到可靠连接 RC transport 和本机实际 TCP transport；
- `Device` 来自当前节点，不要复制另一台机器的 device 名称。

如果只看到 TCP、看不到 RC，先修复 rdma-core/NIC/UCX 安装，不要继续 TQ 集成。

## 4. 构建 TQ native UCX extension

TQ 的 UCX 接入是可选 native extension，必须显式构建：

```bash
cd /path/to/TransferQueue
TQ_BUILD_UCX=1 TQ_UCX_HOME="${TQ_UCX_HOME}" \
  python setup.py build_ext --inplace
```

也可以在构建 wheel 时使用相同的变量：

```bash
TQ_BUILD_UCX=1 TQ_UCX_HOME="${TQ_UCX_HOME}" \
  python -m build
```

验证 native extension 使用的是目标 UCX：

```bash
ldd transfer_queue/_ucx*.so | grep -E 'libucp|libuct|libucs|libucm'
```

所有节点都要重复构建或部署同一 ABI 的 extension，并确保 Ray worker 能加载对应的
`libucp.so`。官方 RPM 可能把库放在 `lib64`，TQ discovery 已支持 `lib` 和 `lib64`。

## 5. 启用 TQ UCX payload

使用 OmegaConf 覆盖 SimpleStorage 配置：

```python
from omegaconf import OmegaConf
import transfer_queue as tq

conf = OmegaConf.create({
    "backend": {
        "SimpleStorage": {
            "payload_transfer": "ucx",
        },
    },
})

tq.init(conf)
```

或者在配置文件中设置：

```yaml
backend:
  storage_backend: SimpleStorage
  SimpleStorage:
    payload_transfer: ucx
```

必须在启动 Ray/TQ 进程前，把 UCX 的 `PATH`、`LD_LIBRARY_PATH` 和 TQ Python/native
extension 配置到每个参与节点。推荐让 TQ 自动发现本机 RDMA device、netdev 和 GID：

```bash
unset UCX_TLS UCX_NET_DEVICES UCX_IB_GID_INDEX UCX_IB_ADDR_TYPE
```

如果部署平台必须显式设置 UCX 变量，配置要表达完整的 RC + TCP auxiliary 候选：

```bash
export UCX_TLS=rc_verbs,tcp,sm,self
export UCX_NET_DEVICES='<rdma_device>:<port>,<tcp_netdev>'
export UCX_IB_GID_INDEX='<gid_index>'
export UCX_IB_ADDR_TYPE=ib_global
```

`<rdma_device>`、`<tcp_netdev>` 和 `<gid_index>` 必须在每个节点按本机实际设备填写。
不要只设置 `UCX_TLS=rc_verbs`：某些 verbs 环境的 RC endpoint 需要 TCP auxiliary 完成
wireup，纯 RC 会出现 `no auxiliary transport` 或 `Destination is unreachable`。

显式设置会覆盖 TQ 自动选择；TQ 不会把用户写的纯 RC 配置偷偷改成 TCP。若不确定，清空
这些变量并使用 TQ discovery。

## 6. 验证 TQ 真正在走 RDMA

仅看到 PUT/GET 成功不能证明使用了 RDMA。启动测试时打开 UCX 日志：

```bash
export UCX_LOG_LEVEL=info
```

有效的 UCX Host RDMA 日志应包含类似：

```text
tag(rc_verbs/<rdma_device>:<port>) ka(tcp/<tcp_netdev>)
```

判定规则：

- `tag(rc_verbs/...)` 或等价 RC transport 是数据 lane，才算 Host RDMA 数据路径；
- `ka(tcp/...)`、wireup TCP 是辅助通道，不等于 TCP data fallback；
- 如果日志是 `tag(tcp/...)`，只能算 TCP 测试，不能计入 RDMA 结果；
- 如果出现 `no auxiliary transport`，优先检查 TCP netdev 是否包含在
  `UCX_NET_DEVICES`，以及对应端口/GID 是否在节点间可达。

仓库内的验证工具：

```bash
# standalone native payload path
python tools/test_ucx_payload_transfer.py --help

# real SimpleStorage actor/manager path
python tools/test_simplestorage_ucx_integration.py --mode ucx

# 在已有 Ray 集群上运行时，把 native extension 和 UCX 库路径传给 Ray worker
export TQ_RAY_WORKER_PYTHONPATH="$(pwd)"
export TQ_RAY_WORKER_LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"
python tools/test_simplestorage_ucx_integration.py --mode ucx --ray-address=auto

# native payload throughput (不替代 SimpleStorage E2E 性能)
python tools/bench_ucx_payload_transfer.py --help
```

SimpleStorage 验证至少应包含：

1. 大 payload PUT 后远端数据校验；
2. GET 后内容和长度校验；
3. CLEAR 后再次 GET 确认数据已删除；
4. UCX 日志确认实际 data lane；
5. 与同拓扑、同 payload 的默认 ZMQ 结果对照。

## 7. 故障定位顺序

### UCX 初始化失败

```text
no RoCE-v2 device and GID match local IP
```

检查本机控制 IP 是否确实落在目标 RoCE netdev，检查 `/sys/class/infiniband`、GID 类型
和 `ip -br addr`。多候选设备时不要在代码里硬编码选择，先明确部署的本机网络拓扑。

### RC endpoint 无法建立

```text
no auxiliary transport
ucp_ep_create() failed: Destination is unreachable
```

确认 `ucx_info -d` 能看到 TCP，并且 TCP netdev 和 RDMA device 同属可互通网络平面。纯
`UCX_TLS=rc_verbs` 不是通用的修复方式。

### TQ 能初始化但数据走 TCP

检查日志中的 `tag(...)`：

- `tag(tcp/...)`：UCX 没有选到 RC，检查 UCX capabilities、`UCX_TLS` 和设备过滤；
- `tag(rc_verbs/...) ka(tcp/...)`：数据已走 RC，TCP 只是 auxiliary；
- 不要只根据吞吐判断 transport，必须以 UCX lane 日志和数据校验为准。

### Ray worker 找不到 UCX

Controller 节点能执行 `ucx_info` 不代表远端 SimpleStorageUnit 能加载 UCX。确认每个
Ray 节点都有：

- 相同 TQ native extension ABI；
- 相同官方 UCX runtime 或兼容的 ABI；
- `PATH`、`LD_LIBRARY_PATH` 和 Python package 可见；
- 正确的本机网络和 GID。

## 8. 代码入口

| 文件 | 责任 |
| --- | --- |
| `setup.py` | `TQ_BUILD_UCX`/`TQ_UCX_HOME` 控制 native extension 构建 |
| `transfer_queue/storage/payload_transfer/factory.py` | 选择 `zmq` 或 `ucx` |
| `transfer_queue/storage/payload_transfer/ucx_discovery.py` | 本机 RDMA/GID/netdev 和 UCX 能力探查 |
| `transfer_queue/storage/payload_transfer/ucx.py` | TQ payload contract 到 UCX 的适配 |
| `transfer_queue/storage/payload_transfer/ucx_runtime.py` | UCP worker、endpoint、request 和生命周期 |
| `transfer_queue/csrc/ucx/ucx_bindings.cpp` | 最小 pybind11/UCP Tagged binding |
| `transfer_queue/storage/bootstrap/simple_storage_bootstrap.py` | 为 SimpleStorage actor 发布 payload transfer metadata |
| `transfer_queue/storage/simple_storage.py` | PREPARE/READY/COMMIT/CANCEL 和数据校验 |

新增 transport 时先确认它是否真的共享 SimpleStorage 的控制/数据面边界；Mooncake、
openYuanrong 和 HIXL 不应被强行改造成 `PayloadTransfer` 的实现。
