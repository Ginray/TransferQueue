# 官方 UCX v1.22.0：A2 Host H2H 验证记录

日期：2026-08-19

本记录只描述未修改的官方 UCX release，不代表 TQ 的默认部署配置。当前 A2 机器只保留
`/opt/tq-ucx/1.22.0-official`；历史测试目录 `/opt/tq-ucx/1.18.1` 和
`/opt/tq-ucx/11307` 已删除。

开发者要复现安装和 TQ 使用流程，请先看
[`ucx_rdma_developer_guide.md`](ucx_rdma_developer_guide.md)；本文只保存本次 A2 实验的
具体结果。

## 环境

| 项目 | 值 |
| --- | --- |
| UCX | 1.22.0，revision `8a6b06f` |
| A2-26 | `178.123.4.4`, `hns_0:1`, `enp189s0f0` |
| A2-27 | `178.123.4.3`, `hns_0:1`, `enp189s0f0` |
| GID | index `3`，IPv4-mapped RoCE GID |

UCX runtime 使用官方 AArch64 RPM 包安装，未修改 UCX 源码。测试时设置：

```bash
export TQ_UCX_HOME=/opt/tq-ucx/1.22.0-official
export PATH="$TQ_UCX_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$TQ_UCX_HOME/lib64:$TQ_UCX_HOME/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export UCX_IB_GID_INDEX=3
export UCX_IB_ADDR_TYPE=ib_global
```

## 原生 UCX 结果

### 纯 RC 失败

```bash
UCX_TLS=rc_verbs
UCX_NET_DEVICES=hns_0:1
ucx_perftest -t tag_bw ...
```

结果：

```text
no auxiliary transport ... Unsupported operation
ucp_ep_create() failed: Destination is unreachable
```

这确认当前 HNS 环境的 UD auxiliary 仍不可用；官方 v1.22.0 没有自动消除这个硬件/驱动
组合上的限制。

### RC 数据 + TCP 辅助通道成功

```bash
export UCX_TLS=rc_verbs,tcp,sm,self
export UCX_NET_DEVICES=hns_0:1,enp189s0f0
```

`ucx_perftest -t tag_bw -s 1048576 -n 20 -w 2` 跨 A2-26/A2-27 成功，日志为：

```text
tag(rc_verbs/hns_0:1) ka(tcp/enp189s0f0)
Final: 20 ... 110.33 MB/s
```

因此实际数据通道仍是 RC，TCP 只承担 wireup/keepalive 辅助通道。之前只配置
`hns_0:1` 时 TCP 不在候选设备中，不能形成辅助通道。

## TQ SimpleStorage 结果

`transfer_queue._ucx` 使用官方 v1.22.0 头文件和库重新构建，在相同环境下完成跨节点
SimpleStorage 操作：

```text
UCX Version 1.22.0
ucp_context ... tag(rc_verbs/hns_0:1) ka(tcp/enp189s0f0)
cross-node manager PASS ... bytes=1048576
cross-node CLEAR PASS
```

本次测得 1 MiB PUT 约 20.97 MiB/s，GET 约 62.03 MiB/s；该数据只是功能验证样本，不是
吞吐承诺。

## 对 TQ 代码的结论

当前 `ucx_discovery.py` 已有正确的基本逻辑：

1. 根据本机 RoCE GID 找到 `hns_0:1` 和 `enp189s0f0`；
2. 通过 `ucx_info -d` 检查 runtime 是否同时提供 RC 和 TCP；
3. 未显式设置 `UCX_TLS` 时生成 `rc_verbs,tcp,sm,self`；
4. 自动生成 `UCX_NET_DEVICES=hns_0:1,enp189s0f0`。

本次只补充了官方 RPM 常见的 `lib64` 目录发现，避免 `ucx_info` 依赖库位于 `lib64` 时
自动能力探测失败。显式设置 `UCX_TLS=rc_verbs` 仍然会被视为用户的强制覆盖，TQ 不会
替用户偷偷追加 TCP；部署时不要用纯 RC 配置。

因此，支持本次方式不需要新增 transport 组件，也不需要修改 UCX；使用当前 TQ 代码并
确保 UCX runtime 可发现、TCP 网卡包含在 `UCX_NET_DEVICES` 即可。
