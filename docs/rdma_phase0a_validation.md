# RDMA Phase 0A 验证记录

> 本文件只保存特定测试集群的历史实验条件和结果。路径、IP、NIC、GID、版本和环境变量
> 均不是 TransferQueue 产品配置，不应复制到部署代码或用户配置。

状态：**No-Go**。Phase 0A 的 Python binding 准入失败；未修改 TransferQueue 数据路径，未进入 Phase 0B。

## 1. 验收目标

- 三台 A2 都有 Conda 环境 `syl-tq-rdma`；
- Python 使用固定的 UCXX 版本，并显式链接 `/opt/tq-ucx/11307`；
- A2-26 与 A2-27 使用 `UCX_TLS=^ud,ud:aux` 完成 Python Tagged Send/Receive；
- 覆盖 64 KiB、1 MiB、16 MiB，重复收发、关闭、重连和进程退出；
- UCX 日志确认实际数据 lane 为 `rc_verbs/hns_*`，TCP 仅作为 auxiliary；
- 任一验收项失败，结果为 No-Go，保持现有 TCP/ZMQ 路径。

## 2. 固定版本与环境

| 项目 | 目标值 | 实际值 |
|---|---|---|
| Python | 3.11 | A2-26/27/28 均为 3.11.15 |
| Conda environment | `syl-tq-rdma` | 三台均为 `/root/ENTER/envs/syl-tq-rdma` |
| UCX prefix | `/opt/tq-ucx/11307` | 三台均已部署并通过 `ucx_info -v` |
| UCX | 1.22.0 + PR #11307 | commit `1554563a574691af7d0342aa8b038a4db77c7034` |
| UCXX | v0.45.01 | C++ core 成功；官方 Python package No-Go |
| UCXX source commit | `c25d2cdb693ca1ccbc72b26cb079c2d0dbe33c20` | 已固定 |

三台环境最终核对结果：

```text
A2-26: Python 3.11.15, /root/ENTER/envs/syl-tq-rdma/bin/python, NumPy 2.4.6, UCX 1.22.0/#11307
A2-27: Python 3.11.15, /root/ENTER/envs/syl-tq-rdma/bin/python, NumPy 2.4.6, UCX 1.22.0/#11307
A2-28: Python 3.11.15, /root/ENTER/envs/syl-tq-rdma/bin/python, NumPy 2.4.6, UCX 1.22.0/#11307
```

## 3. 环境准备命令

三台服务器统一执行：

```bash
source /root/ENTER/etc/profile.d/conda.sh
conda activate syl-tq-rdma
python -V
python -c 'import sys, numpy; print(sys.executable); print(numpy.__version__)'
```

UCX 环境：

```bash
export TQ_UCX_HOME=/opt/tq-ucx/11307
export PATH="$TQ_UCX_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$TQ_UCX_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export UCX_TLS='^ud,ud:aux'
export UCX_IB_GID_INDEX=3
export UCX_IB_ADDR_TYPE=ib_global
export UCX_LOG_LEVEL=info
```

A2-26/27 的 `UCX_NET_DEVICES` 为 `hns_0:1,enp189s0f0`；A2-28 为
`hns_2:1,enp189s0f0`。这里的“尚未进入”特指本节定义的 **UCXX Python binding
Phase 0A**；后续使用 TQ 自有 pybind11 binding 的 native UCX 验证不改变本节对官方
UCXX Python 包的 No-Go 判定。

## 4. UCXX 构建命令

目标是在 `syl-tq-rdma` 内构建 CPU-only UCXX C++ core，关闭测试和 benchmark，显式查找
固定 UCX：

```bash
cmake -S cpp -B cpp/build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" \
  -DCMAKE_PREFIX_PATH="$TQ_UCX_HOME" \
  -DBUILD_TESTS=OFF \
  -DBUILD_BENCHMARKS=OFF \
  -DUCXX_ENABLE_RMM=OFF
ninja -C cpp/build install
```

实际结果：A2-26 的 UCXX C++ core 安装成功，`$CONDA_PREFIX/lib/libucxx.so` 已生成；但这
不能作为 Python binding 通过证据。

Python binding 安装命令：

```bash
python -m pip install --no-build-isolation --no-deps /tmp/ucxx-v04501/python/ucxx
```

实际失败：

```text
ValueError: Could not determine the CUDA version. Make sure nvcc is in your PATH.
```

失败发生在 `Preparing metadata (pyproject.toml)` 阶段，尚未生成 Python extension。这里的结论
不是“UCXX/UCX 整体必须 CUDA”：UCXX C++ core 已经在本环境关闭 RMM 后成功构建，UCXX
文档也支持 `host`/NumPy buffer。实际阻塞的是官方 Python binding 的构建和发行路径。其源码还显示
`python/ucxx/ucxx/_lib/libucxx.pyx` 无条件 `cimport rmm.pylibrmm.device_buffer`，而 v0.45.01
的 Python 包元数据声明 `rmm==25.8.*`。当前 A2 是 Ascend/NPU 环境，没有 CUDA/nvcc，因此不能
把该标准 UCXX Python binding 作为 CPU-only Host binding 使用。

Phase 0A 的关键执行记录：

```text
A2-26: UCXX C++ core cmake + ninja install      PASS
A2-26: UCXX Python metadata/install             FAIL: missing CUDA/nvcc
A2-26/A2-27: Python Tagged Send/Receive         NOT RUN
A2-26/A2-27: 64 KiB / 1 MiB / 16 MiB            NOT RUN
A2-26/A2-27: close / reconnect / process exit   NOT RUN
```

## 5. Python Tagged smoke

测试程序要求：

- server 与 client 分别在 A2-26/A2-27 运行；
- listener/endpoint 使用 UCXX asyncio API；
- 发送 `bytearray` 或 NumPy Host buffer；
- 每个 size 进行 1,000 次收发并校验内容；
- 记录 `UCX_LOG_LEVEL=info`，检查 `rc_verbs/hns_0:1`；
- 关闭 server 后重新启动，再建立 endpoint 完成一轮收发；
- client/server 正常退出，不能有 native crash、未完成 request 或 worker 泄漏。

### 结果

| size | repetitions | content check | data lane | result |
|---:|---:|---|---|---|
| 64 KiB | 1000 | 未执行 | 未执行 | blocked by binding |
| 1 MiB | 1000 | 未执行 | 未执行 | blocked by binding |
| 16 MiB | 1000 | 未执行 | 未执行 | blocked by binding |

| 场景 | 结果 |
|---|---|
| endpoint close | 未执行，blocked by binding |
| server restart/reconnect | 未执行，blocked by binding |
| actor/process exit | 未执行，blocked by binding |
| TCP-only comparison | 未执行，blocked by binding |

## 6. 判定

当前判定：**No-Go**。

UCXX C++ core 和 UCX RC 不能替代 Python binding smoke。由于标准 UCXX Python binding 在
当前 A2 环境无法构建，未执行 Tagged 收发、`rc_verbs/hns_*` 日志、生命周期和重连测试。
按照 RFC 门禁，退出本次 RDMA 目标；不进入 Phase 0B、Phase 1 或 Phase 1.5。

## 7. 后续验证边界

后续阶段没有重新使用 UCXX Python binding，而是采用 TQ 自有 C++/pybind11 UCX 薄封装。
该封装已在 A2-26/A2-27 完成 SimpleStorage 集成验证，并在 A2-26↔A2-28 完成
native Host Tagged 双向 64 KiB、1 MiB、16 MiB 验证；这些结果分别记录在
`docs/rdma_phase2_3_validation.md` 和 RFC 中，不回写为本节的 UCXX Phase 0A PASS。
