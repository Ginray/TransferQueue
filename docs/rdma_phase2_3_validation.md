# RDMA 阶段 2/3 验证记录

> 本文件只保存特定测试集群的历史实验条件和结果。路径、IP、NIC、GID、版本和环境变量
> 均不是 TransferQueue 产品配置，不应复制到部署代码或用户配置。
>
> 说明：2026-08-10 后用户配置已收口为 `payload_transport.enabled`，NIC/GID 改为内部自动
> 解析，UCX 不可用时回退 ZMQ。早期章节中的 `data_plane` 和手工 UCX 环境变量保留为
> 当时实验条件，不代表当前用户配置。
>
> 后续产品收口已删除 GET cache、IOV、timing、progress sleep、阈值覆盖等 `TQ_UCX_*`
> 实验开关，也删除了 HNS 专用 `UCX_TLS` 策略。本文相关章节仅解释历史性能实验，不代表
> 当前代码仍包含这些分支。

日期：2026-08-05

跟进：2026-08-06

## 阶段 2：TQ 自有 UCX Tagged 薄封装

### 环境

- UCX：`/opt/tq-ucx/11307`，UCX 1.22.0，包含 PR #11307 的构建。
- Python：`/root/ENTER/envs/syl-tq-rdma/bin/python`，CPython 3.11，AArch64。
- 扩展：`transfer_queue._ucx`，链接 `libucp/libuct/libucs/libucm`；当次实验构建通过 RPATH 使用 `/opt/tq-ucx/11307/lib`。
- A2-26/A2-27：`UCX_NET_DEVICES=hns_0:1,enp189s0f0`。
- A2-28：`UCX_NET_DEVICES=hns_2:1,enp189s0f0`。
- 通用配置：`UCX_TLS=^ud,ud:aux`、`UCX_IB_GID_INDEX=3`。

### 测试

脚本：[test_ucx_binding.py](../tools/test_ucx_binding.py)

测试顺序为：服务端发布 UCP worker address，客户端建立 endpoint；每个消息按固定 tag 发送并逐字节校验；最后测试 endpoint graceful close，以及无对端时的 timeout/cancel。

| 连接 | 64 KiB | 1 MiB | 16 MiB | close | cancel/timeout |
| --- | --- | --- | --- | --- | --- |
| A2-26 → A2-27 | PASS | PASS | PASS | PASS | PASS |
| A2-28 → A2-27 | PASS | PASS | PASS | PASS | PASS |

每种大小重复 10 次。UCX 日志显示实际创建 `rc_verbs/hns_*:1` 数据接口；`ud_verbs` 被排除后仍能使用 TCP 辅助建连。该结果证明 TQ 的窄 Tagged binding 可以在当前 HNS/UCX 环境传输 Host buffer，但还不是 SimpleStorage 端到端验收，也没有证明相对 ZMQ 的收益。

### 曾发现并修复的问题

1. endpoint close 使用了兼容层的 `UCP_EP_CLOSE_MODE_FLUSH`，在当前 UCX 版本返回 `Invalid parameter`；改为使用默认 graceful close 参数后通过。
2. cancel 后将 `UCS_ERR_CANCELED` 当成普通错误，并且超时清理后析构再次 cancel，触发 UCX async owner 断言；现在取消路径接受该状态，并在异常清理后清空 request 所有权。

## 阶段 3：SimpleStorage 集成

### 已验证

既有 ZMQ 回归在 `/root/miniconda3/envs/syl-rlinf-tq` 环境中运行：

```bash
TORCH_DEVICE_BACKEND_AUTOLOAD=0 \
  python -m pytest -q tests/test_simple_storage_unit.py
```

结果：`15 passed`。这验证了当前修改没有破坏 SimpleStorage 的既有 ZMQ/存储单元路径。

使用脚本 [test_simplestorage_ucx_integration.py](../tools/test_simplestorage_ucx_integration.py) 验证真实 `SimpleStorageUnit` Ray actor 和 manager 数据面，覆盖：

- legacy ZMQ 模式和 UCX 模式；
- 64 KiB 阈值切换；
- 约 8 MiB tensor、numpy、nested tensor、`NonTensorStack`、pickle 值；
- `data_parser` 仍在 StorageUnit 侧执行；
- GET、CLEAR 和缺失数据错误路径。

结果：

- A2-27 同机 legacy ZMQ：PASS；
- A2-27 同机 UCX：PASS；
- A2-27 驱动 A2-28 Ray actor、使用 `UCX_TLS=tcp`：PASS。

跨节点 PASS 的含义是协议、序列化、parser 和生命周期正确；当时 A2-28 容器只暴露 TCP 用户态传输，因此这条结果不是 RDMA PASS。

### RDMA 运行时边界

在 A2-28 容器中临时补入宿主机匹配的 `libibverbs`、`libhns`、`librdmacm`、libnl 和 `libhns-rdmav34.so` 后，UCX 初始化日志出现：

```text
ucp_context_0 inter-node cfg#0 tag(rc_verbs/hns_2:1)
```

这证明容器可以加载 HNS provider，并非 TQ binding 固定退化成 TCP。该次 A2-28 容器实验
本身因服务端进程超时未形成端到端证据；后续 A2-26/A2-27 主机回环和 SimpleStorage
回环结果见下文。

2026-08-06 重新确认 A2-26/A2-27 的 native Worker 可以初始化；随后尝试直接运行独立 UcxDataPlane 回环时，SSH 在认证/命令阶段间歇性超时，无法形成新的有效数据回环证据。使用已有凭据重新尝试时，连接仍在 banner exchange 阶段超时，说明失败发生在远程会话建立层。本次没有修改宿主机驱动、内核或系统库，也没有把失败归因于 TQ 协议；现有有效证据仍止于阶段 2 的 native `rc_verbs` Tagged 测试和阶段 3 的跨节点 TCP SimpleStorage 测试。

后续低风险探测中，A2-26 出现 `kex_exchange_identification: Connection closed by remote host`，A2-27/A2-28 未能在超时时间内返回命令结果。当前不能继续启动新的后台 benchmark，避免在远端状态不明时叠加 Ray/UCX 残留进程。

### 2026-08-06 双向 UcxDataPlane 回环

在 SSH 恢复后，使用独立脚本 [test_ucx_data_plane.py](../tools/test_ucx_data_plane.py) 完成
8 MiB Host payload 的双向回环。服务端和客户端均使用当前 TQ `UcxDataPlane`，不是
`ucx_perftest` 或原生 Verbs 工具。

| 方向 | payload | client | server | UCX data lane |
| --- | ---: | --- | --- | --- |
| A2-27 → A2-26 | 8 MiB | PASS | PASS | `tag(rc_verbs/hns_0:1)` |
| A2-26 → A2-27 | 8 MiB | PASS | PASS | `tag(rc_verbs/hns_0:1)` |

固定环境：`UCX_TLS=^ud,ud:aux`、`UCX_IB_GID_INDEX=3`、
`UCX_NET_DEVICES=hns_0:1,enp189s0f0`。测试中曾出现两类假失败并已定位：服务端 30 秒
等待窗口不足以覆盖 SSH/scp 延迟；A2-26 残留旧 UCP address 时客户端会连接已关闭的旧 TCP
auxiliary 端口。清空旧 address、延长服务端等待时间后，双向回环通过。

两次服务端日志均包含 `data_plane server PASS`，两次客户端日志均包含
`data_plane client PASS`；客户端 UCX 日志分别显示 `tag(rc_verbs/hns_0:1)`。该证据覆盖
UCP address 交换、endpoint 建立、Tagged payload 发送、接收校验和关闭，不覆盖 Ray actor
调度或 SimpleStorage manager 的控制面。

该结果证明 TQ 自有 binding 和 `UcxDataPlane` 可以在当前 HNS HIP08 主机上完成实际
`rc_verbs` 数据传输；SimpleStorage 的跨节点 Ray actor 结果和性能对比见下文。

### SimpleStorage Ray actor 的 GID 配置问题

首次启动跨节点 SimpleStorage UCX actor 时，Ray actor 在 A2-26 创建 UCP worker 失败：

```text
ucp_worker_create: Address not valid
uct_iface_open(rc_verbs/hns_0:1) failed: Address not valid
```

根因是测试配置只传递了 `UCX_TLS` 和 `UCX_NET_DEVICES`，没有把 A2 RoCE 所需的
`UCX_IB_GID_INDEX=3`、`UCX_IB_ADDR_TYPE=ib_global` 传入 actor。独立 host-level 测试之所以
通过，是因为这两个变量由 shell 显式设置。现已将 `gid_index`、`addr_type` 加入 TQ UCX
配置映射和集成测试，随后完成了 SimpleStorage actor 回环重跑。


修复后重跑结果：

```text
enabled=True actor ready on 178.123.4.4 data_address_len=217 type=bytes
enabled=True large PUT done ... throughput_mib_s=33.66
enabled=True large GET done ... throughput_mib_s=59.86
simple_storage enabled=True PASS
```

覆盖大 tensor、numpy、nested tensor、`NonTensorStack`、pickle、`data_parser`、小 payload
inline/ZMQ、CLEAR 和缺失数据检查。该结果是 SimpleStorage 跨节点 RDMA 功能 PASS。

### 2026-08-06 SimpleStorage 性能对比

本节保留当时实现和环境的历史结果；当前判断以本文末尾 2026-08-10 的同拓扑复测为准。

同一 Ray 集群（head `178.123.4.3:39879`）、同一 StorageUnit 节点（A2-26）、同一
8,000,000-byte tensor payload，先完成一轮功能/预热，再追加 4 轮；追加 4 轮的第 1 轮
作为 warmup 丢弃，表中为追加轮次 2–4 的中位数：

| 路径 | PUT 中位吞吐 | GET 中位吞吐 | 功能 |
| --- | ---: | ---: | --- |
| legacy ZMQ | 68.10 MiB/s | 90.27 MiB/s | 4/4 PASS |
| UCX `rc_verbs` | 46.12 MiB/s | 66.06 MiB/s | 4/4 PASS |

在这组环境和 payload 下，UCX PUT 比 ZMQ 低约 32%，GET 低约 27%；因此当前不能声称
SimpleStorage RDMA 带来收益。UCX 路径虽然避开了 ZMQ 大消息，但增加了控制面握手、UCP
address/endpoint 建立和额外线程调度成本。默认配置应继续保持 `payload_transport.enabled: false`，
除非后续更大 payload、多并发或稳定连接复用测试证明收益。

追加 64 MiB 端到端测试（8,388,608 个 int64 元素）结果：

| 路径 | PUT 吞吐 | GET 吞吐 | 功能 |
| --- | ---: | ---: | --- |
| legacy ZMQ | 96.95 MiB/s | 109.17 MiB/s | PASS |
| UCX `rc_verbs` | 70.24 MiB/s | 82.54 MiB/s | PASS |

64 MiB 下 UCX 仍比 ZMQ 低约 27%/24%。因此当时“功能可用、性能收益 No-Go”的结论在
8 MiB 和 64 MiB 两个端到端 payload 上一致。

补充 4 并发 SimpleStorage 测试：使用一个 A2-26 StorageUnit、4 个独立 Manager，每个
Manager 持有自己的 `UcxDataPlane`，同时执行 8,000,000-byte tensor 的 PUT/GET 并校验
内容：

```text
concurrent PASS concurrency=4 elements=1000000
put_seconds=0.393073 get_seconds=0.422818
put_mib_s=77.64 get_mib_s=72.18
```

并发功能通过，但该轮没有对应的同条件 ZMQ 并发对照，因此只能证明并发正确性，不能证明
并发性能收益。

随后在同一隔离 Ray 集群、同一 A2-26 StorageUnit、同一 4 并发和 8,000,000-byte tensor
条件下补齐对照：

| 路径 | PUT 中位吞吐 | GET 中位吞吐 | 功能 |
| --- | ---: | ---: | --- |
| legacy ZMQ | 97.33 MiB/s | 106.46 MiB/s | PASS |
| UCX `rc_verbs` | 76.60 MiB/s | 75.28 MiB/s | PASS |

该轮 UCX 比 ZMQ 低约 21%/29%。因此单流和 4 并发两种场景下，当前实现都没有显示出
SimpleStorage UCX RDMA 的端到端性能收益。

### 连接重建测试

在 A2-26/A2-27 先后启动两次独立的 `test_ucx_data_plane.py server`，每次由 A2-27
重新读取新 address 并创建新的 client。两轮均输出 `data_plane server PASS` 和
`data_plane client PASS`，实际 UCX 选择为 `rc_verbs/hns_0:1`。

这证明对端进程退出后，由上层重新创建 `UcxDataPlane` 可以恢复通信；当前实现不承诺已有
endpoint 自动重连。

### SimpleStorage actor 重启恢复测试

使用 [test_simplestorage_ucx_restart.py](../tools/test_simplestorage_ucx_restart.py) 在
同一隔离 Ray 集群和 A2-26 StorageUnit 节点执行：

1. 创建第一个 `SimpleStorageUnit` actor，完成 8,000,000-byte tensor 的 UCX PUT/GET。
2. 显式 `ray.kill` 第一个 actor。
3. 在同一节点创建替代 actor，读取新的 ZMQ/UCX metadata，重新创建 Manager 和
   `UcxDataPlane`，再次完成 PUT/GET。

结果：

```text
restart initial PASS actor=TQ_STORAGE_UNIT_8316dbb7 host=178.123.4.4
restart replacement PASS actor=TQ_STORAGE_UNIT_fc15f150 host=178.123.4.4
```

两次数据面日志均选择 `rc_verbs/hns_0:1`。这证明 StorageUnit 进程被替换后，控制面重新
提供 endpoint、上层重新创建 Manager/data plane，可以恢复大 payload 通信；不代表已有
Manager 会自动发现新 actor，也不代表 Ray 集群自身具备透明 actor 恢复语义。

### 生产 bootstrap/controller 路径测试

使用 [test_simplestorage_bootstrap_ucx.py](../tools/test_simplestorage_bootstrap_ucx.py)
调用真实 `tq.init()`，不旁路构造 StorageUnit 或 Manager：

1. `initialize_simple_storage()` 创建 Ray `SimpleStorageUnit` actor。
2. bootstrap 通过 `get_payload_transport_info()` 填充内部 endpoint metadata。
3. `TransferQueueClient` 按正式配置创建 `AsyncSimpleStorageManager` 并完成 controller handshake。
4. 通过正式 Manager 执行 8,000,000-byte tensor 的 UCX PUT/GET。

结果：

```text
bootstrap metadata PASS storage=TQ_STORAGE_UNIT_916b3a96 host=178.123.4.4 data_address_len=217
bootstrap manager roundtrip PASS
bootstrap UCX PASS
```

因此当前代码路径已覆盖配置 → bootstrap → controller → Manager → StorageUnit → UCX
数据面的完整功能链路。

### 多节点正式 bootstrap：按进程选择 NIC

首次用同一个全局 `net_devices=hns_0:1,enp189s0f0` 启动双节点 bootstrap 时，A2-28
上的 actor 因继承 `UCX_NET_DEVICES=hns_2:1,enp189s0f0` 而失败：

```text
DataPlaneError: UCX_NET_DEVICES='hns_2:1,enp189s0f0' conflicts with
SimpleStorage data_plane.ucx
```

这不是网络不通，而是当前配置冲突检查正确地拒绝了“一个设备名覆盖不同节点”的配置。
随后将共享配置中的 `net_devices` 留空，并在每个 Ray 进程启动环境中设置本机值：

- A2-28 head/Manager：`UCX_NET_DEVICES=hns_2:1,enp189s0f0`；
- A2-27 worker/StorageUnit：`UCX_NET_DEVICES=hns_0:1,enp189s0f0`。

使用 [test_simplestorage_bootstrap_ucx_multinode.py](../tools/test_simplestorage_bootstrap_ucx_multinode.py)
重跑正式 `tq.init()`：

```text
bootstrap unit metadata PASS id=TQ_STORAGE_UNIT_8b2aae5d host=178.123.4.5 address_len=217
bootstrap unit roundtrip PASS id=TQ_STORAGE_UNIT_8b2aae5d host=178.123.4.5
bootstrap unit metadata PASS id=TQ_STORAGE_UNIT_89f02fde host=178.123.4.3 address_len=217
bootstrap unit roundtrip PASS id=TQ_STORAGE_UNIT_89f02fde host=178.123.4.3
bootstrap multinode PASS
bootstrap multinode close PASS
bootstrap unit roundtrip PASS id=TQ_STORAGE_UNIT_65083175 host=178.123.4.3
bootstrap unit roundtrip PASS id=TQ_STORAGE_UNIT_d8430331 host=178.123.4.5
bootstrap multinode replacement PASS
```

Manager 日志确认 A2-28 的跨节点路径为 `tag(rc_verbs/hns_2:1)`。该轮历史实现依赖每个
Ray 进程注入本机 `UCX_NET_DEVICES`；当前实现已改为按本机 Ray IP 和 RDMA sysfs 自动解析，
管理员环境变量只保留为诊断覆盖。controller、Manager 和 StorageUnit 所在的 Ray worker
仍必须继承一致的 TQ Python 依赖与源码路径，否则会在
actor creation 阶段因缺少 `zmq` 等依赖失败，这属于部署准入问题而不是 RDMA 传输结果。

### 默认 ZMQ 回归

在不启用 `payload_transport` 的默认配置下执行完整 SimpleStorage KV E2E：

```text
TQ_TEST_BACKEND=SimpleStorage pytest tests/e2e/test_kv_interface_e2e.py
54 passed, 2 warnings
```

这确认新增 UCX 配置和代码不会改变默认 ZMQ 路径的 KV PUT/GET/CLEAR、标签、分区和字段
语义。另有基础 StorageUnit 回归 `15 passed`。

### SimpleStorage TCP fallback

使用 A2-27 作为 driver、明确将 StorageUnit 调度到 A2-26，并设置
`UCX_TLS=tcp`、`UCX_NET_DEVICES=enp189s0f0`，执行 8,000,000-byte payload：

```text
enabled=True actor ready on 178.123.4.4 data_address_len=41 type=bytes
enabled=True large PUT ... throughput_mib_s=25.98
enabled=True large GET ... throughput_mib_s=27.34
simple_storage enabled=True PASS
```

这是正式 SimpleStorage bootstrap/Manager 的跨节点 TCP fallback 证据；它没有使用
`rc_verbs`，不能计入 RDMA 性能结果。

### A2-28 native UCX 覆盖

A2-28 的标准 `syl-tq-rdma` 环境当前没有可用的 torch/TQ Python 运行时，因此在该
环境中不能执行 SimpleStorage actor 测试；在找到可复用的隔离运行时之前，先使用不依赖
torch 的原生 Tagged 脚本 [test_ucx_raw_host.py](../tools/test_ucx_raw_host.py) 验证
A2-26↔A2-28：

```text
raw server PASS size=16777216
raw client PASS size=16777216
```

同一 A2-26↔A2-28 链路补测 64 KiB 和 1 MiB：

```text
raw server PASS size=65536
raw client PASS size=65536
raw server PASS size=1048576
raw client PASS size=1048576
```

A2-26 使用 `hns_0:1`，A2-28 使用 `hns_2:1`，两端 `UCX_TLS=^ud,ud:aux`、GID index 3，
日志和环境证明了第三节点的 Host Tagged native 路径可用；该结果不等价于 A2-28 上
SimpleStorage 端到端验证。

随后补做反向方向 A2-28 → A2-26，使用固定的
`/root/ENTER/envs/syl-tq-rdma/bin/python`（CPython 3.11）和扩展目录
`/tmp/tq-ucx-build/transfer_queue`：

```text
raw client PASS size=65536
raw server PASS size=65536
raw client PASS size=1048576
raw server PASS size=1048576
raw client PASS size=16777216
raw server PASS size=16777216
```

因此当前 A2-26↔A2-28 的 native UCX Host Tagged 验证已经覆盖双向 64 KiB、1 MiB 和
16 MiB；数据面日志使用 `rc_verbs/hns_0:1`（A2-26）和 `rc_verbs/hns_2:1`
（A2-28）。

在反向 1 MiB 重跑中显式打开 `UCX_LOG_LEVEL=info`，得到直接路径证据：

```text
hostname-mfzab ... inter-node cfg#0 tag(rc_verbs/hns_0:1)
hostname-nw5hg ... inter-node cfg#0 tag(rc_verbs/hns_2:1)
raw server PASS size=1048576
raw client PASS size=1048576
```

### A2-28 TQ 运行时准入缺口

A2-28 的 `/root/ENTER/envs/syl-tq-rdma/bin/python` 为 Python 3.11.15，但不包含
`torch`；主机也没有可用的 `conda` 命令。随后发现 A2-28 已有可复用的
`/home/dxq/envs/dxq_fsdpturbo_four_py311`（Python 3.11、Ray 2.56、torch/torch_npu
2.10），仅缺少 TQ 的 `pyzmq`、`tensordict` 等依赖；这些依赖已隔离安装到
`/tmp/tq-a228-deps`，没有修改原环境。因此 A2-28 的标准 `syl-tq-rdma` 环境仍未
准入，但已经可以进行隔离的 Ray SimpleStorage E2E 验证。

### 2026-08-06 A2-27 → A2-28 SimpleStorage E2E

使用全新的 Ray session（head `178.123.4.3:39979`，StorageUnit actor 固定资源标签
`A2_28`）和相同的 Python 3.11/TQ 源码，A2-27 作为 driver，A2-28 作为 actor 节点。
A2-28 actor 使用 `hns_2:1` 配置，A2-27 manager 使用 `hns_0:1`，两端 GID index 3。

8,000,000-byte tensor（约 7.63 MiB）结果：

| 路径 | PUT | GET | 功能 |
| --- | ---: | ---: | --- |
| UCX `rc_verbs` 配置 | 48.56 MiB/s | 65.92 MiB/s | PASS |
| legacy ZMQ | 70.97 MiB/s | 90.06 MiB/s | PASS |

同一 session 下补测 16,000,000-byte tensor（约 15.26 MiB）：

| 路径 | PUT | GET | 功能 |
| --- | ---: | ---: | --- |
| UCX `rc_verbs` 配置 | 62.77 MiB/s | 74.79 MiB/s | PASS |
| legacy ZMQ | 85.50 MiB/s | 100.30 MiB/s | PASS |

两种 payload 均覆盖大 tensor、numpy、nested tensor、`NonTensorStack`、pickle、
`data_parser`、小 payload 的 ZMQ inline、CLEAR 和缺失数据检查。UCX manager 日志明确
选择 `tag(rc_verbs/hns_0:1)`；actor 侧使用 `hns_2:1` 配置，且 A2-26↔A2-28 native
Tagged 日志已明确显示 `tag(rc_verbs/hns_2:1)`。本轮证明了 A2-27→A2-28 的
SimpleStorage 跨节点功能路径可运行，但未证明 UCX 性能收益：在两个 payload 下，
UCX PUT/GET 均低于同条件 ZMQ。

证据边界：本轮 manager 侧 Ray 日志直接包含 `tag(rc_verbs/hns_0:1)`；A2-28 actor 的
配置明确使用 `hns_2:1`，其实际 `rc_verbs/hns_2:1` 选择由同链路 native UCX 日志确认，
但本轮没有从 Ray actor 的独立 stderr 中提取到同一条 transport 选择日志。因此 E2E
功能结果与 native 路由证据结合支持当前判断，后续若要形成逐 actor 的审计证据，应在
StorageUnit 初始化时输出/采集 UCP selected transport。

测试结束后已停止本轮 Ray head/worker；A2-28 的临时依赖、源码和 Ray session 均位于
`/tmp`，不改变宿主机既有环境。

测试输出末尾还出现了 `AsyncSimpleStorageManager` 析构时缺少
`controller_handshake_socket` 的日志。该集成脚本通过 `object.__new__` 绕过正式 controller
bootstrap，属于测试 harness 的清理告警；PUT/GET/CLEAR 已在告警前完成，不能把它解释为
UCX 传输失败。正式 bootstrap/controller 生命周期仍以本文前面的专门测试为准。

### 2026-08-06 反向 A2-28 → A2-27 SimpleStorage E2E

为排除单向路径影响，交换 Ray 拓扑：A2-28 作为 driver/Manager，A2-27 作为
StorageUnit actor；actor 固定资源标签为 `A2_27`。A2-28 manager 使用 `hns_2:1`，
A2-27 actor 使用 `hns_0:1`，两端 GID index 3。UCX manager 日志明确显示：

```text
UCX_NET_DEVICES=hns_2:1,enp189s0f0
inter-node cfg#0 tag(rc_verbs/hns_2:1)
```

端到端结果如下：

| Payload | 路径 | PUT | GET | 功能 |
| ---: | --- | ---: | ---: | --- |
| 8 MiB | UCX `rc_verbs` | 50.17 MiB/s | 65.51 MiB/s | PASS |
| 8 MiB | legacy ZMQ | 71.92 MiB/s | 92.61 MiB/s | PASS |
| 16 MiB | UCX `rc_verbs` | 65.74 MiB/s | 76.24 MiB/s | PASS |
| 16 MiB | legacy ZMQ | 86.31 MiB/s | 100.84 MiB/s | PASS |

该方向同样覆盖 parser、inline 小对象、CLEAR 和数据校验。两端方向的结果均显示
UCX 功能可用但吞吐低于同条件 ZMQ；因此当前 RFC 的“功能 Go、性能收益 No-Go”结论
不依赖单向部署。测试结束后已停止反向 Ray head/worker。

同一反向拓扑再执行 4 个独立 Manager 的并发 PUT/GET，每个 Manager 传输
8,000,000-byte tensor：

| 路径 | 并发 PUT | 并发 GET | 功能 |
| --- | ---: | ---: | --- |
| UCX `rc_verbs` | 79.68 MiB/s | 76.47 MiB/s | PASS |
| legacy ZMQ | 101.26 MiB/s | 104.07 MiB/s | PASS |

UCX 4 个 worker 的日志均选择 `tag(rc_verbs/hns_2:1)`；4 个独立 Manager 的数据校验
全部通过。该结果进一步确认 UCX 的并发功能成立，但在当前链路和实现中仍低于 ZMQ。

### 2026-08-06 反向并发回归（digest 强制版本）

在同一 A2-27 → A2-28 拓扑重新执行 4 并发测试；本轮使用当前代码中
`UcxDataPlane(require_address_digest=true)` 的默认配置，4 个 Manager 各自创建独立
data plane，并传输 1,000,000 个 `int64`（每路 8,000,000 bytes）：

| 路径 | 并发 PUT | 并发 GET | 功能 |
| --- | ---: | ---: | --- |
| UCX `rc_verbs` | 78.25 MiB/s | 72.81 MiB/s | PASS |
| legacy ZMQ | 100.78 MiB/s | 106.22 MiB/s | PASS |

UCX 和 ZMQ 均完成 4 路数据校验；UCX 相比 ZMQ 的 PUT/GET 吞吐分别低约 22%/31%。
这轮结果与前面的单流、反向拓扑和旧并发结果一致：当前实现的 UCX Host RDMA 路径
功能可用，但没有端到端性能收益，因此默认仍应使用 ZMQ。

测试脚本通过 `object.__new__` 构造 Manager，结束时会产生缺少
`controller_handshake_socket` 的析构告警；告警发生在 PUT/GET 完成之后，属于该隔离
harness 的清理问题，不影响本轮功能结论。Ray 临时集群已在测试后停止。

### 反向可靠性与 actor 替换

在 A2-28 driver → A2-27 StorageUnit 拓扑下补做失败和重建路径：

```text
failed PUT expected error=RuntimeError
failed PUT not committed PASS
subsequent valid PUT/GET PASS

restart initial PASS actor=TQ_STORAGE_UNIT_1dfce282 host=178.123.4.3
restart replacement PASS actor=TQ_STORAGE_UNIT_a275be75 host=178.123.4.3
```

失败 PUT 后没有可读的半写对象，随后新对象可正常 PUT/GET；StorageUnit actor 被替换后，
Manager 使用新 ZMQ 信息和新 UCX address 重新建连成功。两次重建的 manager 日志均显示
`tag(rc_verbs/hns_2:1)`。这与正向已有的失败 PUT、actor replacement 结果一致。

Manager 异步层回归：

```text
pytest tests/test_async_simple_storage_manager.py
24 passed
```

这覆盖 Manager 的初始化、ZMQ 操作、clear、hash routing 和错误处理；未启用 UCX 时仍
保持原行为。

Client 层回归：

```text
pytest tests/test_client.py
55 passed
```

这确认新增配置字段和 Manager close/data-plane 生命周期没有破坏上层 Client API、
checkpoint 及错误处理测试。

在 A2-28 的隔离 Python 3.11/TQ 运行时中，使用与 E2E 相同的源码和 `/tmp` 依赖重新执行
三组针对性回归：

```text
pytest test_simple_storage_unit.py test_async_simple_storage_manager.py test_client.py
94 passed in 133.37s
```

该结果确认新增 address guard、异构 NIC 配置路径和 UCX 数据面改动没有破坏默认 ZMQ、
StorageUnit、Manager 或 Client 行为。

在加入 bootstrap `address_digest`、GET receiver digest 和 controller 错误传播后的同一隔离
环境中再次执行，结果仍为：

```text
94 passed in 133.36s
```

### 全量回归

先执行完整测试目录：

```text
556 passed, 10 skipped, 8 errors
```

8 个错误全部来自 `tests/test_yuanrong_storage_client_e2e.py` 的既有 mock fixture：它对
当前 `yuanrong_client` 模块不存在的 `datasystem` 属性执行 `mock.patch`，与本次 UCX 或
SimpleStorage 修改无关。

排除该独立文件后重新执行其余全部测试：

```text
pytest tests --ignore=tests/test_yuanrong_storage_client_e2e.py
556 passed, 10 skipped, 2 warnings
```

因此本次改动范围内的全量回归通过；Yuanrong fixture 问题仍需单独修复，不能标记为全仓
测试完全通过。

本次在当前 checkout 的系统 Python 3.9 环境重跑上述测试时，测试收集阶段即被环境阻断：
缺少 `tensordict`，并且系统 `torch_npu` 找不到 `libhccl.so`；另有 Torch Triton
重复注册错误。因此本机重跑结果是**未执行到测试体**，不是代码失败。回归结论以已记录的
A2-28 隔离 Python 3.11 环境中的 `94 passed` 和此前排除 Yuanrong fixture 后的全量结果为准。

### controller 重建测试

在同一隔离 Ray 集群内连续执行两次正式初始化：

1. `tq.init()` → bootstrap → controller → Manager，完成 PUT/GET。
2. `tq.close()`，关闭旧 Manager、StorageUnit 和 controller。
3. 再次 `tq.init()`，创建新的 controller、StorageUnit、UCX address 和 Manager，重新完成 PUT/GET。

结果：

```text
bootstrap metadata PASS storage=TQ_STORAGE_UNIT_708ee915 host=178.123.4.3 data_address_len=217
bootstrap initial manager roundtrip PASS
controller close PASS
bootstrap metadata PASS storage=TQ_STORAGE_UNIT_263cf942 host=178.123.4.3 data_address_len=217
bootstrap replacement manager roundtrip PASS
controller replacement PASS
```

两轮均使用当前生产代码的 `UCX_IB_GID_INDEX=3`、`UCX_IB_ADDR_TYPE=ib_global` 映射，
实际 UCX 选择为 `rc_verbs/hns_0:1`。该测试证明 controller/StorageUnit/Manager 全部由
上层重新创建后可以恢复通信；不代表已有 Manager 在 controller 崩溃后自动恢复。

测试中曾出现一次 A2-27 `Address not valid`：排查确认远端测试目录残留旧版
`data_plane.py`，只设置了 `UCX_TLS/UCX_NET_DEVICES`，没有当前实现的 GID/地址类型映射。
同步当前生产源码后重跑通过；以后复现实验必须先同步或安装与本地一致的 TQ 源码。

### controller 崩溃时已有 Manager 的行为

单独验证了 controller actor 崩溃，而不关闭已有 Manager、StorageUnit 或 UCX data plane：

```text
controller kill PASS
controller unavailable PASS
existing manager notify failure propagated PASS type=RuntimeError
existing manager after controller crash PASS
```

原 Manager 在 controller 被 `ray.kill` 后，仍能通过原 StorageUnit endpoint 完成新的
8,000,000-byte UCX PUT/GET。这说明 controller 不是已建立数据传输的同步依赖。

随后修复 `notify_data_update()`：controller 返回 `success=false` 或 ACK 超时现在向调用方抛出
可捕获异常，
不再把 metadata 未确认伪装成成功。已有 Manager 的 UCX 数据面仍可在 controller 崩溃后
继续 PUT/GET；该修复不提供 controller 自动重连，metadata 恢复仍需上层重新建立 controller
或另行实现重试协议。

补充同进程重入测试 [test_controller_reinit_after_crash.py](../tools/test_controller_reinit_after_crash.py)：

```text
controller initial init PASS
controller crash observed PASS
same-process tq.init after crash expected error=ActorDiedError
```

这确认当前 `tq.init()` 复用已保存的 dead controller handle，不能在同一进程自动切换到
新 controller；可行恢复方式仍是进程级重新初始化，或后续新增 controller discovery、旧
Manager/StorageUnit 重绑定和 metadata 重放协议。

### UCX 发送失败安全性：生产路径已防护，裸 binding 仍有边界

使用 [test_simplestorage_ucx_failed_put.py](../tools/test_simplestorage_ucx_failed_put.py)
尝试在 PUT 已完成 `PUT_DATA_PREPARE`、但发送端拿到损坏 peer address 的情况下验证失败
是否可捕获。结果不是 Python 异常，而是 native UCX 进程直接 abort：

```text
Assertion `*addr_version == UCP_OBJECT_VERSION_V2' failed: addr version 9
Fatal Python error: Aborted
File "transfer_queue/storage/data_plane.py", line 228 in _endpoint
```

因此原始实现不能把任意 UCX address/endpoint 建连失败安全地转换为 TQ PUT 错误。当前已在
`UcxDataPlane._endpoint()` 增加保守的长度边界：拒绝小于 16 字节或大于 1 MiB 的控制面
address，避免明显截断数据进入 `ucp_ep_create()`；这不是完整的 UCX 二进制格式校验，
裸 binding 的相同长度内容损坏仍保持 No-Go；正式 SimpleStorage 路径另由 bootstrap
digest 防护，见下文。

修复后的短地址和有效地址回归：

```text
malformed address guard PASS: invalid UCX worker address length: 9
valid address regression client_rc=0 server_rc=0
data_plane client PASS
data_plane server PASS
tag(rc_verbs/hns_2:1)
```

这证明已知的 9 字节损坏地址不再触发进程 abort，且新增 guard 不影响有效 8 MiB
`rc_verbs` 传输；裸 binding 的同长度损坏 address 仍由后续 digest envelope 保护，
pending receive 清理已在后续同机和跨节点故障注入中通过。

随后使用 [test_ucx_same_length_corrupt.py](../tools/test_ucx_same_length_corrupt.py) 篡改
合法 address 的 version/header 字节，长度保持不变，结果仍会触发 native abort：

```text
same_length_version_corrupt_rc=134
Assertion `*addr_version == UCP_OBJECT_VERSION_V2' failed: addr version 9
```

补充探测还发现，篡改中间的非 header 字节在本地 self endpoint 场景下可能仍被 UCX 接受；
因此不能把长度 guard 当作完整校验，也不能把所有同长度损坏都假设为同一种错误。裸
binding 的非法 header/version 仍可能让 native UCX abort；正式 SimpleStorage 已使用
可信 digest envelope 绕开该 native 输入边界。

对 `/opt/tq-ucx/11307/lib/libucp.so` 做符号审计时可以看到
`ucp_address_unpack`、`ucp_address_length` 等导出符号，但安装包的 public headers 没有
对应声明。它们不能作为稳定的 UCP public API 直接接入 TQ；当前实现不依赖这些内部符号，
而是使用 worker address 获取能力加上 TQ digest/长度校验。

### SimpleStorage 生产地址 envelope 回归

为避免裸 binding 的 native abort 进入正式 SimpleStorage 路径，bootstrap 现在同时发布
`address` 和 `address_digest=sha256(address)`。Manager PUT、StorageUnit GET 发送前校验
digest，不匹配时不会调用 `ucp_ep_create()`；`UcxDataPlane` 默认
`require_address_digest=true`，缺少 digest 也会在 Python 层拒绝。A2-27↔A2-28 故障注入结果：

```text
corrupt bootstrap address expected error=DataPlaneError
corrupt bootstrap address guard PASS state={'pending_puts': 0, 'pending_gets': 0, 'pending_receives': 0}
failed PUT remote cleanup PASS state={'pending_puts': 0, 'pending_gets': 0, 'pending_receives': 0}
corrupt GET receiver expected error=RuntimeError
corrupt GET receiver guard PASS state={'pending_puts': 0, 'pending_gets': 0, 'pending_receives': 0}
subsequent valid PUT/GET PASS
```

独立 binding guard 也通过：

```text
malformed address guard PASS: invalid UCX worker address length: 9
same-length digest guard PASS: UCX worker address digest does not match bootstrap metadata
```

因此当前结论改为：正式 SimpleStorage bootstrap 路径对已篡改 address 为 Go；裸
`Worker.connect()` 或不提供 digest 的底层诊断调用仍可能触发 UCX native abort，不作为
生产接口使用。GET 反向发送路径也已验证：StorageUnit 拒绝损坏 receiver address，
Manager 的本地 receive 被取消，原对象仍可正常读取。

补充了更接近实际网络故障的测试：使用格式有效、但已经关闭的 peer address。结果为：

```text
failed PUT expected error=RuntimeError
failed PUT not committed PASS
subsequent valid PUT/GET PASS
```

该场景证明正常 endpoint 失联会返回可捕获错误，不提交失败对象，且后续有效传输可以恢复。

### PUT 失败后的远端资源清理

为关闭上述 pending receive 缺口，新增 `PUT_DATA_CANCEL` 控制消息。Manager 在
`PUT_DATA_READY` 之后、COMMIT 成功之前的异常路径发送 cancel；StorageUnit 同时移除
`_pending_puts` 并调用 `UcxDataPlane.cancel_receive()`。验收脚本现在还读取 StorageUnit
内部诊断计数，不能只用“对象不可读”间接判断清理结果。

A2-27 同机重跑结果：

```text
failed PUT expected error=RuntimeError
failed PUT remote cleanup PASS state={'pending_puts': 0, 'pending_gets': 0, 'pending_receives': 0}
failed PUT not committed PASS
subsequent valid PUT/GET PASS
```

随后将 StorageUnit 调度到 A2-28，Manager 在 A2-27，使用 A2-28 的
`hns_2:1,enp189s0f0` 配置重跑，结果相同；说明跨节点 UCX PUT 失败后的远端 descriptor 和
native receive 均已释放。此次测试的失败注入是格式有效但已关闭的 peer address，因此仍
不能覆盖非法 header/version address 的 native abort 问题。

### 变更后的正常路径回归

在 A2-27 同机 Ray 集群中，针对新增 cancel 和 controller 错误传播代码重新执行 legacy 与
UCX 两条路径，覆盖 8,000,000-byte payload、parser、nested tensor、NonTensorStack、
pickle、small inline PUT/GET 和 CLEAR：

```text
simple_storage enabled=False PASS
simple_storage enabled=True PASS
enabled=True large PUT done seconds=0.096804 payload_bytes=8000000 throughput_mib_s=78.81
enabled=True large GET done seconds=0.058255 payload_bytes=8000000 throughput_mib_s=130.96
```

该回归确认新增协议没有改变正常 ZMQ、UCX、parser 或 CLEAR 语义。

### digest 接入后的正式双节点 bootstrap 回归

在 A2-27 driver/head 与 A2-28 StorageUnit worker 上，使用异构设备
`hns_0:1`/`hns_2:1` 和共享配置 `net_devices=""`，重新执行两轮正式 bootstrap、PUT/GET、
`tq.close()` 和重建：

```text
bootstrap unit metadata PASS id=... host=178.123.4.3 address_len=287
bootstrap unit roundtrip PASS id=... host=178.123.4.3
bootstrap unit metadata PASS id=... host=178.123.4.5 address_len=217
bootstrap unit roundtrip PASS id=... host=178.123.4.5
bootstrap multinode PASS
bootstrap multinode close PASS
bootstrap multinode replacement PASS
```

两节点的 address digest 均由当前 bootstrap 生成并被 Manager/StorageUnit 使用；该结果
确认 digest 字段没有破坏异构 NIC、正式生命周期或 actor 重建路径。

## 当前结论

- 阶段 1：环境和 native 扩展准入通过。
- 阶段 2：UCX Host Tagged 数据面和 TQ `UcxDataPlane` 双向 Host 回环通过，Go；日志证明 A2 主机路径使用 `rc_verbs`。
- 阶段 3：SimpleStorage 的正式 bootstrap、controller 重建、controller 崩溃后的已有 Manager 数据面、正常 endpoint 失联恢复、legacy/UCX 同机、跨节点、4 并发、actor 替换后重新建连、PUT 失败后的同机/跨节点远端资源清理，以及生产 bootstrap address digest 防护均通过；同时确认 controller 自动重连和 metadata 恢复尚未实现，裸 UCX binding 在不提供 digest 时仍可能触发 native abort。当前同拓扑 8 MiB 五轮对照已观察到 UCX 单流 GET 比 ZMQ 快约 7.7%，但尚未覆盖完整消息大小/并发矩阵，不能将收益推广为默认结论。

下一步按以下顺序执行：

1. 若要支持底层裸 binding 的任意地址输入，需要进一步实现 native 可捕获的 UCX address 校验；`PUT_DATA_CANCEL`、address digest 和 ACK/拒绝错误传播已通过验证，但 controller 自动重连和 metadata 一致性恢复仍未实现。
2. 当前仍保持默认 ZMQ；UCX 作为显式 opt-in，继续完成消息大小、并发和 workload 总成本矩阵后，再决定是否调整默认路径。

性能脚本会输出 `large PUT/GET seconds` 和 `throughput_mib_s`。正式比较至少执行 legacy ZMQ 与 UCX 各 5 次，丢弃第一次 warmup，使用相同 actor 节点、相同 payload 和相同 Ray 资源约束；单次成功不能作为收益结论。

### 测试环境清理记录

本轮独立 Ray 集群使用 head `178.123.4.3:39879` 和临时目录
`/tmp/tq-ray-rdma-20260806`，测试结束后已停止。清理时发现 A2-26 还存在一个此前连接
到 `178.123.4.2:6379` 的旧 Ray worker；该 worker 也被 Ray 清理命令停止。按停止前记录
尝试恢复时，A2-26 当前仅有 Ray 2.56，而旧集群要求 Ray 2.48，版本检查拒绝重新加入：

```text
Version mismatch: cluster Ray 2.48.0, local Ray 2.56.0
```

因此没有强行绕过版本检查或覆盖环境；该旧 worker 的恢复需要原 Ray 2.48 安装包或由原
集群管理员恢复。该环境影响与本次 UCX/SimpleStorage 功能结果分开记录。

### 对端退出生命周期测试

使用 [test_ucx_data_plane.py](../tools/test_ucx_data_plane.py) 的 `server_exit`/`peer_exit`
角色验证：A2-27 创建 UCP worker、发布 address 后立即退出，A2-26 使用当前
`UcxDataPlane` 尝试发送。结果为 `data_plane peer_exit PASS`；UCX 日志显示实际选择
`tag(rc_verbs/hns_0:1)`，并在 auxiliary TCP 连接被拒绝后快速返回异常。该测试证明对端
退出不会让 TQ 数据面永久阻塞；它不等价于自动重连，重连仍需由上层重新创建 data plane。

### 16 MiB transport scale test

使用新增脚本 [bench_ucx_data_plane.py](../tools/bench_ucx_data_plane.py) 在 A2-27↔A2-26
执行 16 MiB、5 次重复的独立 transport 测试：

```text
bench server PASS size=16777216 repetitions=5
bench client PASS size=16777216 repetitions=5 median_seconds=0.184408 throughput_mib_s=86.76
UCX: tag(rc_verbs/hns_0:1)
```

同样在 64 MiB、5 次重复下通过：

```text
bench server PASS size=67108864 repetitions=5
bench client PASS size=67108864 repetitions=5 median_seconds=0.724871 throughput_mib_s=88.29
UCX: tag(rc_verbs/hns_0:1)
```

这证明较大 Host payload 和重复传输稳定；benchmark 不包含 Ray、ZMQ 控制面、编码/解码，
因此不能直接替代 SimpleStorage 端到端性能结论。

### digest 强制后的原生跨节点回归

在启用 `UcxDataPlane` 默认 `require_address_digest=true` 后，重新执行 A2-27→A2-28
原生 Tagged transport；客户端显式使用 `sha256(address)`，服务端和客户端均完成数据校验：

```text
data_plane server PASS
data_plane client PASS
bench server PASS size=16777216 repetitions=5
bench client PASS size=16777216 repetitions=5 median_seconds=0.174975 throughput_mib_s=91.44
bench server PASS size=67108864 repetitions=5
bench client PASS size=67108864 repetitions=5 median_seconds=0.690974 throughput_mib_s=92.62
```

该结果证明 digest 校验只约束建连前的控制元数据，不改变已建立的 `rc_verbs` 数据 lane；
吞吐数据仍是独立 transport 基准，不代表 SimpleStorage 端到端收益。

### 2026-08-06 性能拆分与优化结果

为区分控制面和数据面，在 Manager 的 UCX PUT/GET 路径增加了可选的
`TQ_UCX_TIMING=1` 分阶段打点，并完成了以下优化：

- `pack_frames()` 保留 `bytearray`，native binding 接受 contiguous buffer，避免发送侧
  `bytearray → bytes → std::vector` 的额外复制；
- PUT 的 UCX send 与等待 `PUT_DATA_READY` 并行；
- StorageUnit 在 GET PREPARE 完成编码后立即异步启动反向 UCX send，GET COMMIT 仍等待
  send future 完成，保持错误确认语义；
- Manager 在 GET COMMIT 后并行等待 ZMQ response 和本地 UCX receive，避免两段等待串行；
- native receive buffer 改为未初始化分配，避免每次接收前对整个 Host payload 做无意义清零；
  该改动已通过跨节点功能回归，但在当前 8 MiB 端到端测试中没有表现出稳定的独立收益；
- native binding 使用显式 `ucp_worker_progress()` 加固定 50 us 的可控让出；尝试完全忙轮询
  时原生 16 MB 吞吐从约 99.6 降到约 70.0 MiB/s，因此未作为默认配置。

为确认 completion loop 是否引入了 GET 延迟，在同一 A2-27/A2-28 Ray 集群、8 MiB payload
下串行测试 A2-27 Manager native binding 的 `TQ_UCX_PROGRESS_SLEEP_US`；每组 2 次，
第一轮预热。A2-28 StorageUnit 保持默认 50 us，因此该组数据只用于判断 Manager 侧
send/receive completion，不代表双端参数对照：

```text
sleep_us=0:  PUT 90.30 MiB/s, GET 86.33 MiB/s
sleep_us=1:  PUT 89.70 MiB/s, GET 85.50 MiB/s
sleep_us=10: PUT 88.49 MiB/s, GET 84.48 MiB/s
sleep_us=50: PUT 89.66 MiB/s, GET 85.75 MiB/s
```

结果没有随 sleep 缩短而稳定提升，故保留 50 us 默认值；该参数仅作为诊断开关，不写入
生产配置。双端完全一致的参数对照仍需在 A2-28 binding 同步后单独执行。

同一 Ray 集群、A2-27 → A2-28、同一 StorageUnit 节点、同一 payload，默认 UCX 参数执行 3
次；第一轮仅作预热。当前结果如下：

```text
8 MB payload, current run:
UCX PUT: 81.43, 90.77, 94.48 MiB/s
UCX GET: 79.52, 85.64, 88.64 MiB/s
ZMQ PUT: 75.03, 87.29, 88.96 MiB/s
ZMQ GET: 90.56, 92.66, 93.36 MiB/s

16 MB payload, current run:
UCX PUT: 92.91, 98.61, 100.12 MiB/s
UCX GET: 91.58, 94.45, 97.03 MiB/s
ZMQ PUT: 88.96, 96.06, 97.89 MiB/s
ZMQ GET: 100.59, 101.98, 102.15 MiB/s
```

结论：

1. PUT 已达到并超过 ZMQ：8 MB 稳态约快 3~6%，16 MB 稳态约快 2.5%。
2. GET 8 MB 稳态约慢 5~8%；16 MB 稳态约慢 6%，说明 GET 仍受编码/控制握手和
   UCX Host 发送路径共同影响，不能宣称所有方向和大小都已获益。
3. 当前 8 MB GET 的打点为：`prepare_ready≈16.1~20.3 ms`、
   `commit_response≈69.6~70.4 ms`、`ucx_receive≈69.6~70.3 ms`、
   `decode≈1.2 ms`。`commit_response` 和 `ucx_receive` 基本重叠，说明此前的
   串行等待已消除；但 PREPARE 阶段仍位于数据传输之前，约增加 16~20 ms。
4. 因此可以作出量化归因：当前 GET 端到端约 87.6~93.5 ms，其中约 70 ms 是 UCX
   数据面，约 16~20 ms 是 PREPARE 前置阶段（包含编码、描述符/控制握手及调度），
   解码约 1.2 ms。控制面不是唯一瓶颈，但 PREPARE 前置开销确实解释了 UCX 原生
   transport 与 TQ GET 之间的大部分额外差距。
5. 原生 transport benchmark 已修正为“先 post receive、再发布 address”、使用与 TQ
   相同的 mutable `bytearray` payload，并移除服务端完整 payload 校验对后续请求的阻塞；
   8 MB、5 次重复达到约 `101.33 MiB/s`，与 TQ `ucx_send_ms≈70~73 ms` 一致。

使用新增脚本 [bench_zmq_multipart.py](../tools/bench_zmq_multipart.py) 的纯 transport
对照中，8 MB、5 次重复的 ZMQ multipart + 接收端 ACK 曾达到 `111.41 MiB/s`；UCX
Tagged RC 原生 transport 仍低约 9%，但 TQ PUT 通过控制面与数据面重叠后端到端已经
超过 ZMQ。`get_zcopy`/`put_zcopy` 在当前 HNS/UCX 栈上会导致测试 actor 异常退出，
`host:1M/2M/4M` rendezvous fragment 调整没有稳定收益；单 rail、pipeline 也没有稳定
收益。batch 32/64 会触发 `rc_verbs_iface.c` 的 `alloca` assertion，故这些参数均未写入
默认配置。

### 2026-08-07 GET 编码缓存优化

当前 GET 的 `PREPARE` 阶段每次都会执行 `StorageUnitData.get_data()`、`encode()` 和
`pack_frames()`。对于同一组 `fields + global_indexes` 被重复读取的场景，实验实现增加了
有界编码缓存。该开关现在只保留为内部诊断环境变量，不属于用户配置：

```bash
export TQ_UCX_GET_CACHE_ENTRIES=4
```

默认值为 `0`（关闭），不改变现有内存占用和行为。缓存只保存达到
`inline_threshold_bytes` 的 UCX payload；PUT/CLEAR 现在按字段和 global index 定向失效，
LOAD checkpoint 仍清空全部缓存，避免旧数据继续被发送。这样写入无关 key 或无关字段时，
已有热点 GET 可以继续命中。开启后应重点观察 StorageUnit 日志中的
`cache_hit=True` 与 `encode_pack_ms`，并用重复 GET 的端到端基准比较收益。
缓存关闭时不会构造索引 tuple，也不会为默认路径增加索引复制开销。
该优化不能消除 `StorageUnitData.get_data()` 和 UCX 传输耗时，也不能用于绕过调用方
直接原地修改已存对象的场景；这类修改必须通过 TQ PUT 让缓存失效。

现有集成脚本可用以下环境变量执行同一 key 的重复 GET：

```bash
TQ_UCX_GET_CACHE_ENTRIES=4 TQ_UCX_REPEATED_GETS=3 \
python tools/test_simplestorage_ucx_integration.py --mode ucx --ray-address <ray-address>
```

另外，UCX GET PREPARE 的 `StorageUnitData.get_data()` 只是字典引用收集，不涉及
PyTorch 张量聚合，因此移除了该路径上的 `limit_pytorch_auto_parallel_threads()`。
这样不会在每个 GET 上反复调用 `torch.set_num_threads()`；PUT、CLEAR 和 legacy ZMQ GET
路径保持原有线程控制逻辑。该改动需要在 A2 节点用 `TQ_UCX_TIMING=1` 对比
`storage_get_ms` 后再确认收益。

同时复用 `encode()` 已产生的 `frame_count`，不再在每次 GET 后调用
`unpack_frames()` 重新解析 descriptor 元数据。

### 2026-08-07 UCX IOV GET 实现

native binding 新增了可选的 Tagged IOV send/receive：StorageUnit 直接发送
`encode()` 产生的 frame 列表，Manager 按 `frame_sizes` 接收。该路径绕过
`pack_frames()` 和 `unpack_frames()` 的整块 Host buffer 拷贝，配置开关为：

```bash
TQ_UCX_GET_USE_IOV=1 python tools/test_simplestorage_ucx_integration.py \
  --mode ucx --ray-address <ray-address>
```

默认配置仍为关闭。A2-27/A2-28 的 `/opt/tq-ucx/11307` native binding 均已重编译；
A2-27 同机 IOV 64 KiB、1 MiB、16 MiB 各 10 次通过，A2-27→A2-28 使用 UCX TCP 的
IOV 跨节点校验也通过。真实 SimpleStorage 同机 IOV GET/CLEAR 流程通过，100000 个
int64（约 0.8 MiB）的一次 GET 观测为：

```text
UCX IOV GET: total_ms=9.253, throughput=72.31 MiB/s
StorageUnit: storage_get_ms=0.011, encode_ms=2.241
```

这只是同机功能和单次性能样本，不能代表 RoCE 跨节点收益。A2-27→A2-28 使用当前
`^ud,ud:aux`/HNS 选择时仍在 `ucp_ep_create: Destination is unreachable` 失败，
需要先解决 UCX/HNS 建连问题。IOV 默认保持关闭，待 RoCE 组通过后再与 contiguous
Tagged 路径做同一节点、同一 payload、同一重复次数的对照。

同一 A2-27、同一 8 MiB payload 的单次对照为：

```text
contiguous GET: 21.807 ms, 349.86 MiB/s
IOV GET:        20.458 ms, 372.93 MiB/s
```

而约 0.8 MiB payload 的对照为 contiguous `8.176 ms / 93.31 MiB/s`、IOV
`10.551 ms / 72.31 MiB/s`。因此 IOV 不是全尺寸段的无条件优化，当前配置增加
`get_iov_min_bytes: 4194304`，建议只让 4 MiB 以上 payload 走 IOV；该阈值仍需在
RoCE 跨节点环境用多次重复测试重新标定。

当同时设置 `get_payload_cache_entries > 0` 时，IOV 路径也缓存 frame 列表，重复 GET
会跳过 `get_data()` 和 `encode()`；低于 `get_iov_min_bytes` 的 contiguous fallback
也复用连续 payload 缓存，缓存失效规则与 contiguous 缓存一致。

本次继续优化了 UCX PUT 后的首次 GET：当 `data_parser` 为 `None`、缓存已开启，且
GET 的字段顺序和 `global_indexes` 与 PUT 完全一致时，StorageUnit 会复制 PUT 的已编码
payload 预热对应缓存。这样首个匹配 GET 可以直接进入发送路径，不再重复执行
`get_data()`、`encode()` 和 frame 打包。预热不会复用 UCX receive buffer，而是复制到独立的
Python-owned buffer，避免 PUT 完成后出现悬空内存引用；启用 `data_parser` 时明确跳过，
因为 parser 可能改变实际存储值。

这个优化只降低 GET 延迟，不等价于端到端总成本降低：复制 payload 和 frame 元数据解析的
成本被前移到 PUT COMMIT。必须同时报告 PUT、首个 GET 以及 PUT+GET 总耗时；如果 workload
是一次写入、一次读取，默认关闭缓存更合理。当前集成脚本可用
`TQ_INTEGRATION_USE_PARSER=0` 验证该条件，使用 parser 的默认测试仍覆盖原有语义。
脚本会打印 StorageUnit 的 `cache_stats`；连续路径预热后应看到
`contiguous_entries > 0`，IOV 路径且 payload 达到 IOV 阈值时应看到
`iov_entries > 0`。

A2-27 同机、8 MiB、`get_payload_cache_entries=1` 的重复 GET 样本：

```text
GET #0: 20.937 ms / 364.39 MiB/s, cache_hit=False
GET #1: 17.814 ms / 428.29 MiB/s, cache_hit=True
GET #2: 16.550 ms / 461.00 MiB/s, cache_hit=True
```

StorageUnit 的 `encode_ms` 从首次约 `1.972 ms` 降为命中时 `0 ms`；该缓存只适合重复
读取且写入均经过 TQ PUT/CLEAR 的场景。

### 2026-08-07 缓存预热与定向失效复测

在 A2-27、Python 3.11、CPU Torch、`UCX_TLS=sm,self` 的真实
SimpleStorage 集成中，使用 `TQ_INTEGRATION_USE_PARSER=0`、缓存容量 1，得到以下单次
样本。这里是同机 UCX 功能和路径验证，不是跨节点 RoCE 性能结论：

| 路径 | Payload | 首个 GET | 后续/无关 PUT 后 GET | 预热缓存 |
|---|---:|---:|---:|---|
| contiguous | 8 MiB | 11.13 ms | 8.38 ms | `contiguous_entries=1` |
| IOV | 8 MiB | 18.82 ms | 14.16 ms | `iov_entries=1` |
| IOV fallback | 0.8 MiB | 6.00 ms | 4.33 ms | `contiguous_entries=1` |

IOV 0.8 MiB 样本确认低于 `get_iov_min_bytes` 时，fallback 也能复用连续缓存；8 MiB
样本确认 IOV 预热使用 frame cache。写入无关 key `102` 后再次读取热点 key `101` 成功，
说明定向失效没有误清理不相关缓存。

同样的 8 MiB contiguous 测试开启 `data_parser`、但保持默认
`get_prewarm_parser=false` 时，首个 GET 日志为 `cache_hit=False`，后续 GET 才命中；
因此默认 PUT 预热不会绕过 parser 语义。开启 parser-aware opt-in 后的独立结果见本文末尾。
以上各组均打印 `simple_storage enabled=True PASS`。

Manager 的 GET receive 等待也改为直接包装 UCX owner thread 返回的
`concurrent.futures.Future`，不再为每次 GET 经由 `asyncio.to_thread()` 进入默认线程池。
这只减少 Python 调度层开销，不改变 UCX completion 或数据生命周期。改动后的 A2-27 IOV
8 MiB 集成仍通过：首个 GET `18.46 ms`、第二个 GET `16.57 ms`，并且无关 small PUT
后的热点 GET 仍通过；该样本尚未与同一环境下的旧实现做严格多轮 A/B，因此不单独宣称
固定百分比收益。

同一 A2-27、8 MiB、contiguous、无 parser 的 5 次 GET 对照中，缓存关闭时端到端样本为
`17.68, 15.72, 14.96, 13.38, 12.23 ms`；缓存开启并由 PUT 预热后为
`10.73, 9.49, 7.53, 7.69, 7.23 ms`。这是单机 `sm,self` 样本，仍需跨节点 RoCE
多轮复测；但它确认缓存收益来自跳过 StorageUnit 编码，而不是只看吞吐瞬时抖动。

同机 4 路并发、每路 8 MiB 的 UCX PUT/GET 集成也通过，GET 总吞吐为 `810.60 MiB/s`。
当前 native worker 仍使用单 owner thread，但已改为通过 `Request.test()` 轮询多个
in-flight request，不把每个异步 send/receive 串行阻塞在 `Request.wait()`。

### 2026-08-07 跨节点 RoCE GET 优化复测

使用 A2-27 driver/Manager、A2-28 StorageUnit，固定资源 `A2_28`，两端代码 hash 一致，
网卡分别为 `hns_0:1,enp189s0f0` 和 `hns_2:1,enp189s0f0`，UCX 配置为
`UCX_TLS=rc_verbs,tcp,sm,self`、GID index 3。这里显式加入 TCP，是因为本轮使用
`^ud,ud:aux` 时日志出现 `rc_verbs/... - no connect to iface` 和
`no auxiliary transport`；显式 TCP 后才形成稳定的 RC + TCP auxiliary 建连。

8 MiB、无 parser、同一 Ray session 的跨节点结果：

| 路径 | GET #0 | GET #1 | GET #2 | StorageUnit 缓存 |
|---|---:|---:|---:|---|
| contiguous + cache | 80.38 ms | 76.36 ms | 73.58 ms | `contiguous_entries=1` |
| IOV + cache | 80.20 ms | 75.39 ms | 73.78 ms | `iov_entries=1` |
| contiguous，无 cache | 91.66 ms | 81.80 ms | 80.24 ms | disabled |

同一 A2-28 actor 的 legacy ZMQ 对照为 `84.56/82.80/81.91 ms`，功能同样通过。缓存
明显减少了 UCX StorageUnit 的编码开销；此前样本不能据此宣称 UCX 慢于 ZMQ，正式结论
需要同一轮多次对照。

三组均打印 `simple_storage enabled=True PASS`；contiguous 和 IOV 的首次 GET 均由 PUT
预热命中，且无关 small PUT 后热点 GET 仍通过。IOV 在该真实 RoCE 链路上没有超过
contiguous，当前只能确认它绕过了 frame packing，不能宣称额外网络吞吐收益。

同一拓扑 4 路并发、每路 8 MiB 的 UCX GET 也通过，aggregate GET 为 `813.41 MiB/s`。
这证明 owner-thread Future 没有破坏跨节点并发；但该值与同机测试不可直接比较。本轮
legacy ZMQ 已完成 3 次串行对照，正式发布结论仍应按 benchmark 规范执行至少 5 次并丢弃
首轮 warmup。

### 2026-08-07 并发 GET 控制面解耦（已由 2026-08-08 顺序修订取代）

`GET_DATA_PREPARE` 已经启动异步 UCX send，因此 `GET_DATA_COMMIT` 不再同步等待
`send_future`。StorageUnit 立即返回 `GET_DATA_RESPONSE`，由 Manager 的 UCX receive
完成作为数据成功确认；send future 的异常仍记录在 StorageUnit 日志中。该修改主要针对
并发 GET：避免单个慢传输阻塞 StorageUnit 的单一控制 worker。需要在 A2 上用串行和并发
两组分别测量，不能仅凭串行 GET 判断收益。

完成 native 非阻塞轮询后，跨节点 4 路 8 MiB GET 仍功能通过；本次复测为
`get_seconds=0.301442`、aggregate `101.24 MiB/s`。两台机器的 `enp189s0f0` 均协商为
`1000 Mb/s`，单流约 69~70 ms 已接近该链路线速，因此本次没有观察到 4 路吞吐线性提升。
这说明多 request progress 改动没有破坏并发，但也不能凭代码优化突破当前网络带宽；此前
`813.41 MiB/s` 样本不再作为当前稳定基线。

### 2026-08-07 五轮 GET 对照与 parser 预热

在同一 Ray session、A2-27 Manager → A2-28 StorageUnit、8 MiB、无 parser、连续 payload
缓存开启的条件下，UCX 使用 `UCX_TLS=rc_verbs,tcp,sm,self`，UCX 日志明确选择
`tag(rc_verbs/hns_0:1)`。UCX GET 五轮为 `80.39, 78.16, 76.72, 74.90, 74.69 ms`；
丢弃首轮后平均约 `76.12 ms`。同拓扑 legacy ZMQ 五轮为
`85.12, 82.90, 82.71, 82.18, 82.25 ms`；丢弃首轮后平均约 `82.51 ms`。
在这组固定条件下，UCX GET 比 ZMQ 快约 7.7%，因此当前结论应改为：UCX Host RoCE
数据面已经观察到单流 GET 收益，但收益依赖拓扑、缓存状态和消息大小，尚不能推广为所有
负载的收益。

UCX GET 计时中稳态 `ucx_receive_ms` 约 `69.5~70.0 ms`，而 StorageUnit 缓存命中时
编码准备约 `0.05~0.08 ms`、decode 约 `1.3~1.5 ms`；后续优化重点应放在传输/协议，
而不是重复调 `encode/pack_frames`。`UCX_RNDV_SCHEME=get_zcopy` 的三轮结果与默认
方案相近（稳态约 `75.17~79.97 ms`），`TQ_UCX_PROGRESS_SLEEP_US=0` 也未显示稳定
收益，暂不把这些参数写入默认配置。

新增 `get_prewarm_parser` 后，在 parser 开启、8 MiB、缓存开启的跨节点验证中：PUT 后
缓存条目为 `contiguous_entries=1`，首次 GET `79.69 ms`，第二次 GET `75.00 ms`，两次
均通过 parser 结果校验（`reference=[7,7,7]`）。这证明缓存保存的是 parser 后的已提交
值，而不是原始 PUT payload；该样本同时显示 PUT/GET 总成本仍需按 workload 的重复次数
单独评估，不能只看首个 GET。

### 2026-08-08 代码审计后复测

拓扑保持 A2-27 Manager → A2-28 StorageUnit，8 MiB Host payload，两端使用
`/opt/tq-ucx/11307`，配置为 `UCX_TLS=rc_verbs,tcp,sm,self`。Manager 日志再次确认
`tag(rc_verbs/hns_0:1)`。

无 parser、连续缓存、同一 Ray session 五轮结果：

| 路径 | GET 五轮（ms） | 丢弃首轮平均 | PUT 丢弃首轮平均 |
| --- | --- | ---: | ---: |
| UCX | 77.56 / 75.09 / 73.79 / 73.97 / 73.35 | 74.05 ms | 80.92 ms |
| ZMQ | 83.82 / 81.87 / 83.37 / 81.76 / 81.26 | 82.06 ms | 81.03 ms |

该组 UCX GET 比 ZMQ 低约 9.8%；稳态 PUT 差异不明显。UCX 稳态
`ucx_receive_ms` 为 69.33–69.54 ms，StorageUnit cache-hit prepare 为
0.056–0.064 ms，瓶颈仍是 1 Gb/s 网络传输。

四路并发、每路 8 MiB：UCX GET 104.18 MiB/s，ZMQ GET 104.73 MiB/s；两者都已到达
链路上限，没有并发扩展收益。

故障注入覆盖损坏 bootstrap address、已退出 peer、失败 PUT 不提交、损坏 GET receiver
address 和后续有效 PUT/GET，所有检查通过，最终状态均为
`pending_puts=0/pending_gets=0/pending_receives=0`。将对端 address 校验移到异步 request
提交前，损坏 GET receiver 由等待 receive timeout 改为立即返回控制面 `RuntimeError`。

本轮代码同时完成：UCX 关闭时不再为默认 ZMQ PUT 预先执行一次无用的
`encode()+pack_frames()`；本地 worker address 在创建时缓存；descriptor 对
transfer id、tag、非负长度、frame count 和 IOV 总长进行结构校验。

### 2026-08-08 协议状态与异步取消复测

在同一 A2-27 Manager → A2-28 StorageUnit 拓扑补充了状态归属、资源上限和孤儿回收测试。
首次测试发现 native `Request.cancel()` 会在 UCX owner thread 中同步等待取消完成；取消
未匹配 GET send 后会占住 GIL，后续 ZMQ 控制请求超时。另一个问题是标准
`concurrent.futures.Future.cancel()` 对已进入 running 的请求无效。修复后使用支持运行中
cancel-request 的 Future，所有 send/receive/timeout
取消均拆为 `start_cancel()` 与 `test_cancel()`：owner thread 持续 progress，控制线程不等待
native cancel completion，请求对象保留到 UCX 返回终态后再释放。

验证结果：

- GET cancel 后 `pending_gets=0`；
- 非 owner sender 不能取消 PUT，原 owner 可取消且 receive 被回收；
- 本轮历史结果中，`max_transfer_bytes=16 MiB` 时 16 MiB+1 descriptor 会在 native
  分配前被拒绝。后续自动分块实现已把该字段改为单个 native Tagged request 上限：
  超限逻辑 payload 携带 `chunk_bytes` 并透明拆分；新的硬上限为 `max_payload_bytes`；
- 未提交 PUT 在 1 秒 deadline 后自动回收，后续控制请求可正常响应；
- 3 轮 8 MB PUT、每轮 2 次 GET、parser 预热、small ZMQ fallback 和 CLEAR，legacy/UCX 均通过；
- 损坏 bootstrap、失败 PUT、损坏 GET receiver 后 pending 三项均为 0，随后有效 PUT/GET 通过。

本轮 8 MB 稳态样本中，legacy GET 约 80.97–81.52 ms，UCX GET 约
74.19–75.20 ms；UCX PUT 首轮 161.55 ms，后两轮 85.41/86.55 ms，首轮包含 endpoint
建立成本。该样本用于回归确认，不替代前述五轮性能结论。

最初 eager GET 在返回 READY 前发送；若 Manager 此时取消，发送可能已经进入接收端
unexpected queue，发送端 cancel 无法撤回。最终实现改为 GET PREPARE 只保存发送计划，
Manager post receive 后以 GET COMMIT 触发异步 send。复测四项协议用例全部通过且不再出现
unexpected-tag 告警。3 轮 8 MB 回归中，legacy GET 为 81.39–83.76 ms，UCX GET 为
75.02–78.11 ms；未观察到增加一次控制顺序后的明显退化。

### 自动分块专项验证

代码侧可用小阈值触发真实分块，不需要分配 GiB 数据：

```bash
export TQ_UCX_MAX_TRANSFER_BYTES=1048576
export TQ_UCX_MAX_PAYLOAD_BYTES=67108864
export TQ_INTEGRATION_ELEMENTS=1000000
python tools/test_simplestorage_ucx_integration.py --mode ucx --ray-address "$RAY_ADDRESS"
```

该用例约 8 MiB tensor 会触发多块 PUT 和 GET，并校验 tensor、NumPy、NestedTensor、
NonTensorStack、pickle、parser、small inline 和 CLEAR。实机验收还必须确认 UCX 日志选择
`tag(rc_verbs/...)`，并在结束后检查 pending PUT/GET/receive 均为 0。分块 descriptor、
tag、直接写入逻辑 buffer、组合完成/取消已有本地单测；A2 实机结果见下一节。

### 2026-08-10 当前代码复测与审查

拓扑为 A2-27 Manager（`hns_0:1`）→ A2-28 StorageUnit（`hns_2:1`），两端使用
`/opt/tq-ucx/11307` 和隔离源码 `/tmp/tq-rdma-20260810-src`。每组执行 6 轮，以下取
丢弃首轮后的吞吐中位数；无 parser，GET cache 开启并由 PUT 预热：

| Payload | ZMQ PUT | UCX PUT | ZMQ GET | UCX GET |
| --- | ---: | ---: | ---: | ---: |
| 8 MiB | 95.15 MiB/s | 95.78 MiB/s | 94.06 MiB/s | 101.81 MiB/s |
| 16 MiB | 102.52 MiB/s | 99.78 MiB/s | 102.30 MiB/s | 106.40 MiB/s |
| 64 MiB | 109.40 MiB/s | 101.07 MiB/s | 109.28 MiB/s | 106.50 MiB/s |

Manager UCX 日志确认 `tag(rc_verbs/hns_0:1)`。相对 ZMQ，UCX GET 在 8/16 MiB
分别快约 8.2%/4.0%，64 MiB 慢约 2.5%；UCX PUT 在 8 MiB 基本相当，16 MiB 慢约
2.7%，64 MiB 慢约 7.6%。旧版 64 MiB 的 24%～27% 差距已不再复现，但当前仍不能
声明所有尺寸和方向均有收益。

64 MiB 关闭 cache 后，UCX PUT/GET 中位数约为 103.53/103.23 MiB/s。开启 cache
主要把约 18～20 ms 编码成本从 GET 移到 PUT 的预热复制：一次 PUT + 一次 GET 的总耗时
基本不变；只有同一 payload 重复 GET 时才有明确净收益。因此 cache 默认保持关闭。

使用 `max_transfer_bytes=4 MiB` 对约 64 MiB 编码 payload 做自动分块，实际产生 17 个
Tagged request。跨节点 PUT/GET 和数据校验通过；稳定 PUT 约 103.67 MiB/s，GET 的
UCX 网络阶段约 588.2 ms，与未分块约 587.9～588.0 ms 基本一致。分块没有显示明显的
网络吞吐损失，端到端 GET 波动主要来自 19～55 ms 的编码阶段。

本轮代码审查同时完成：

- active UCX request 存在时，owner thread 不再阻塞任务队列最多 1 ms，空闲时才阻塞等待；
- 任一分块失败时主动取消其余 sibling request；
- `with_chunking()` 对分块和未分块 descriptor 使用同一校验入口；
- 删除已不可达的“多临时 buffer 再整包复制”分支，接收结果始终是 native 直接填充的
  单个逻辑 buffer；
- timing 输出增加 chunk count，便于实机确认自动分块。

上述结果仍受 1 Gb/s 链路限制。下一步性能优化优先级不是继续调整 UCX fragment 参数，
而是根据 workload 选择 cache、增加应用级 inflight bytes/backpressure，并在更高速 RoCE
网络上重新比较；当前 `get_zcopy`、多 rail 和 IOV 均没有稳定收益证据。

### 2026-08-10 用户配置收口

用户配置改为单一开关：

```yaml
payload_transport:
  enabled: false
```

NIC、RDMA device/port、RoCE-v2 GID、HNS transport 组合、分块和安全上限改为内部处理。
在 A2-27/28 清空 `UCX_NET_DEVICES`、`UCX_IB_GID_INDEX`、`UCX_IB_ADDR_TYPE` 和 `UCX_TLS`
后，解析和 worker 创建结果为：

```text
A2-27 hns_0:1,enp189s0f0 3 address_len=217
A2-28 hns_2:1,enp189s0f0 3 address_len=217
```

本地无 UCX extension 环境使用同一最小配置启动成功，Manager 未创建 UCX transport，
小对象 PUT/GET 经 ZMQ 完成。自动回退只发生在传输开始前；进行中的 PUT/GET 不透明重放。
A2-27 隔离源码曾在不设置 `TQ_BUILD_UCX/TQ_UCX_HOME` 时自动发现 `/opt/tq-ucx/11307`，
native extension 强制重编译通过，ELF RPATH 为 `/opt/tq-ucx/11307/lib`。该行为仅证明当次
实验构建可用，现已从通用构建逻辑移除：A2 验证镜像应在构建 wheel 时提供 UCX SDK 前缀，
不能让产品代码识别固定实验目录或把构建机绝对路径固化进发布物。
