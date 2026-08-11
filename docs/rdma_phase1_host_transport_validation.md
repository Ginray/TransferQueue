# Phase 1：Host H2H transport 验证记录

> 本文件只保存特定测试集群的历史实验条件和结果。路径、IP、NIC、GID、版本和环境变量
> 均不是 TransferQueue 产品配置，不应复制到部署代码或用户配置。

日期：2026-08-04；跟进：2026-08-06  
状态：**Host RDMA transport 验证通过；本文件记录独立 transport 基线。TQ 自有
UCX binding 和 SimpleStorage 集成结果见 `rdma_phase2_3_validation.md`。**

## 1. 阶段结论

当前 A2 集群的 Host RoCE 链路本身是通的。不能把 UCX 的默认 `ud_verbs` 建链问题解释成底层 RDMA 不可用：

- UCX 1.22.0 + PR #11307：`tag_bw` 已在 A2-27↔A2-28 完成 64 KiB、1 MiB、16 MiB 收发；日志显示数据 lane 为 `rc_verbs/hns_*:1`，TCP 仅用于 auxiliary/wireup。
- 原生 `libibverbs` RC：`ib_write_bw` 和 `ib_read_bw` 已在 A2-26↔A2-27 完成 64 KiB、1 MiB，并在 A2-27↔A2-28 完成 16 MiB；日志显示 Ethernet、GID index 3、RC QP。
- 因此，当前 TQ Host H2H 的工程选型应为：**UCX C++ backend 作为主实现，原生 Verbs RC 作为基准和 fallback**。UCX 已在当前 HNS 上通过 #11307 实际走通 `rc_verbs`，且性能与 Verbs 基线一致；直接 Verbs 虽然可用，但会把 QP 建链、rkey 交换、重连和错误恢复全部转移给 TQ。
- 本文件的独立 transport 结果不是 TQ 的正确性或收益证明。后续阶段已完成 TQ 自有
  binding、SimpleStorage 跨节点 RDMA 功能、并发、生命周期和 ZMQ 对比；性能结论仍为
  当前测试中 UCX 未优于 ZMQ。

## 2. 环境与固定路径

| 项目 | 值 |
| --- | --- |
| A2-26 Host RoCE | `178.123.4.4`, `hns_0:1`, `enp189s0f0` |
| A2-27 Host RoCE | `178.123.4.3`, `hns_0:1`, `enp189s0f0` |
| A2-28 Host RoCE | `178.123.4.5`, `hns_2:1`, `enp189s0f0` |
| UCX | `/opt/tq-ucx/11307`, 1.22.0, commit `1554563a574691af7d0342aa8b038a4db77c7034` |
| UCX 构建 | `--with-verbs --with-rdmacm --without-cuda --without-rocm --enable-mt` |
| Verbs 工具 | `/usr/bin/ib_write_bw`, `/usr/bin/ib_read_bw` |
| GID | index 3，IPv4-mapped RoCE GID |

UCX 实验使用：

```bash
export TQ_UCX_HOME=/opt/tq-ucx/11307
export PATH="$TQ_UCX_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$TQ_UCX_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export UCX_TLS='^ud,ud:aux'
export UCX_IB_GID_INDEX=3
export UCX_IB_ADDR_TYPE=ib_global
export UCX_LOG_LEVEL=info
```

A2-26/27 使用 `UCX_NET_DEVICES=hns_0:1,enp189s0f0`，A2-28 使用
`UCX_NET_DEVICES=hns_2:1,enp189s0f0`。

## 3. UCX 结果

测试命令（server 在 A2-27，client 在 A2-28）：

```bash
# server
ucx_perftest -p <port> -t tag_bw -s <size> -n 200

# client
ucx_perftest 178.123.4.3 -p <port> -t tag_bw -s <size> -n 200
```

| 方向 | size | 结果 | client overall |
| --- | ---: | --- | ---: |
| A2-28 → A2-27 | 64 KiB | PASS | 110.02 MB/s |
| A2-28 → A2-27 | 1 MiB | PASS | 109.97 MB/s |
| A2-28 → A2-27 | 16 MiB | PASS | 110.37 MB/s |

关键日志：

```text
UCX 1.22.0 (loaded from /opt/tq-ucx/11307/lib/libucp.so.0)
UCX_TLS=^ud,ud:aux ... UCX_NET_DEVICES=hns_2:1,enp189s0f0
perftest inter-node cfg#0 tag(rc_verbs/hns_2:1)
Final: 200 ... 110.02
Final: 200 ... 109.97
```

这验证的是 **UCX Tagged Send/Receive 的 RDMA 数据路径**，不是 UCX RMA；本阶段不以
`ucp_put_bw` 作为验收项。

## 4. 原生 Verbs RC 结果

测试命令（server 在 A2-27，client 在 A2-26）：

```bash
# server
ib_write_bw -d hns_0 -i 1 -x 3 -s <size> -n 500 -p <port>
ib_read_bw  -d hns_0 -i 1 -x 3 -s <size> -n 500 -p <port>

# client
ib_write_bw 178.123.4.3 -d hns_0 -i 1 -x 3 -s <size> -n 500 -p <port>
ib_read_bw  178.123.4.3 -d hns_0 -i 1 -x 3 -s <size> -n 500 -p <port>
```

| 操作 | size | 结果 | average bandwidth |
| --- | ---: | --- | ---: |
| RC Write（本地写远端） | 64 KiB | PASS | 109.85 MB/s |
| RC Write（本地写远端） | 1 MiB | PASS | 110.34 MB/s |
| RC Read（本地读远端） | 64 KiB | PASS | 110.34 MB/s |
| RC Read（本地读远端） | 1 MiB | PASS | 110.35 MB/s |
| RC Write（本地写远端） | 16 MiB | PASS（A2-28 → A2-27） | 110.37 MB/s |
| RC Read（本地读远端） | 16 MiB | PASS（A2-28 → A2-27） | 110.37 MB/s |

关键日志包含：

```text
Connection type : RC
Link type       : Ethernet
GID index       : 3
rdma_cm QPs     : OFF
```

因此，原生 Verbs 已覆盖与 TQ `PUT`/`GET` 对应的两种单边方向：Write 和 Read。

## 5. 对 TQ 选型的影响

### 后续阶段结果（2026-08-06）

使用 TQ 自有 `pybind11` binding 的 native Tagged 脚本补测 A2-26↔A2-28 双向链路，覆盖
64 KiB、1 MiB、16 MiB，全部通过；`UCX_LOG_LEVEL=info` 显示两端实际数据 lane 分别为
`tag(rc_verbs/hns_0:1)` 和 `tag(rc_verbs/hns_2:1)`。A2-28 当前没有 torch/TQ Python
运行时，因此该节点只完成 native transport 验证，不能替代 SimpleStorage E2E 验证。

### 当前 Go

可以进入 TQ Host H2H backend 的实现设计，主路径固定为 UCX Tagged Send/Receive；原生 Verbs RC 只用于交叉校验和 UCX 不可用时的后续 fallback。当前仍不能声称 TQ 已集成 RDMA 或已经获得端到端加速。

### 当前 No-Go

- UCXX Python binding：此前因 CUDA/nvcc/RMM 的官方构建路径失败，仍不作为 Python 前端。
- HIXL：当前实验依赖 Ascend/HCCL/HCCN 资源，不适合作为 CPU-only SimpleStorage Host backend。
- UCX Python 直接接入：没有可用且已验证的 Python binding；应通过 TQ 自有的 C++/pybind11 薄 binding 或独立 helper 进程接入。

### 下一道门禁

1. 为 A2-28 补齐与 A2-26/A2-27 匹配的 torch、torch_npu、Ray 和 TQ 运行时。
2. 在三节点上补跑 SimpleStorage UCX RDMA E2E，并保留与 ZMQ 的同条件对照。
3. 处理当前已知可靠性缺口：损坏 UCX address 可能触发 native abort，以及失败 PUT
   缺少显式 cancel/abort 协议。

## 6. 注意事项

- 可用 [phase1_host_rdma_bench.sh](../tools/phase1_host_rdma_bench.sh) 重跑三种数据量；脚本只使用独立端口，并将 server 日志保存到 `/tmp`。
- 本轮 A2 SSH 通过代理出现过 `kex_exchange_identification`，个别后台 benchmark 端口因此残留；残留只属于本轮测试进程，不能作为通信失败证据。
- 约 110 MB/s 是当前 Host RoCE 测试链路的实测值，不是 TQ 的吞吐承诺，也没有证明 RDMA 相比 ZMQ 的端到端收益。
- 在 TQ 实现前，不应把 `UCX_TLS=rc_verbs,tcp,...` 视为自动解决方案；必须看到实际 `rc_verbs/hns_*` 数据 lane 并完成数据校验。
