# NIXL-UCX Payload 生命周期与免拷贝设计

## 范围

本文说明 `NixlPayloadTransfer` 的 Host payload 数据面。ZMQ 仍负责
PREPARE、READY、COMMIT、CANCEL 和错误响应；Controller、路由及公共存储接口不变。

这里的“免拷贝”指：`encode()` 产出的 frames 进入 payload transfer 后，直到接收端
`decode()` 交付结果，不再为网络传输拼接或复制整包数据。调用 transfer 之前的分片、
设备到 Host 搬运，以及 transfer 返回后的 batch 重组不在此范围内。

## 当前状态

| 目标 | PUT | GET | 当前结论 |
| --- | --- | --- | --- |
| 控制面与数据面分离 | ZMQ 只传协议消息，NIXL WRITE 传 frames | 同左 | 已完成，未改变 TQ 公共接口和调度逻辑 |
| 发送路径免整包拷贝 | 连续可写或只读 frame 直接注册 | 同左；来自 receive MR 的 frame 复用原 MR | 已完成；非连续 frame 有明确复制降级 |
| 接收路径免整包拷贝 | NIXL scatter 直接写 registered buffer | 同左 | 已完成，不再执行 receive detach copy |
| decoded data 所有权 | buffer 随已存数据存活 | buffer 随调用方返回值存活 | 已完成，最后一个 view 释放后才能复用 |
| Buffer Pool | StorageUnit 缓存一个空闲 buffer | manager 最多缓存已知 StorageUnit 数量 | 已完成，best-fit 复用；上限只约束空闲 buffer |
| MR 复用 | receive MR 在 pool/lease 间复用 | receive MR 既用于接收，也可作为后续 GET 源 MR | 已完成；外部 source 仅在本次 transfer 临时注册 |
| Metadata 大小 | endpoint 只发布稳定连接信息，PREPARE 发布当前 receive MR | 同左 | 已完成，避免反复携带全部存量 registration |
| PREPARE/CANCEL/异常 | pending state 与 receive buffer 成对创建和清理 | 取消确认远端停止后才回收本地 receive buffer | 已完成，优先保证不会复用仍可能被写入的内存 |
| Transfer 清理 | handle 成功释放后才反注册 source MR | 同左 | 已完成；反注册失败时保留 owner 到 agent teardown |
| 计时观测 | encode、控制面、注册和数据面分段记录 | receive、decode/store 分段记录 | 保留；仅 `TQ_PAYLOAD_TIMING=1` 时收集 |
| 超时与后台回收 | 不新增 | 不新增 | 有意不做，避免按大 payload 耗时推断 peer 失效 |

## 数据路径

发送端直接把 encoded frames 作为 NIXL scatter/gather 源：

- 连续可写 frame 直接取得地址。
- 连续只读 frame 通过 NumPy view 取得地址，不复制数据。
- 非连续 frame 才复制到连续的临时 buffer。
- 如果 GET 的 frame 位于仍处于租用状态的 receive MR 中，直接复用该 MR；其他
  frame 临时注册，并在 transfer handle 成功释放后反注册。

接收端从 buffer pool 取得一个已注册 buffer，预先写入 frame table，并将每个 frame
的远端 descriptor 指向该 buffer 中对应的区域。NIXL 直接写入这些区域，不需要先接收
到独立 frames 再拼接。

COMMIT 后，`receive()` 返回 registered buffer 的 view。`unpack_from()` 只生成切片，
tensor 和 ndarray 分别通过 `torch.frombuffer`、`numpy.frombuffer` 解码，因此不会再做
整包 detach copy。

## Buffer 所有权

每个 `NixlRuntime` 维护一个有界的空闲 receive buffer pool：StorageUnit 缓存一个，
manager 最多按已知 StorageUnit 数量缓存。PREPARE 优先取能容纳 payload 的最小 buffer；
pool 满时保留较大的 buffer，以适应 RL 训练中重复出现的稳定 batch shape。

完成接收后，buffer 不会立即回到 pool。decoded tensor/ndarray 持有其 exporter，finalizer
只在最后一个 view 释放后归还 buffer：

- PUT 中，buffer 随写入 `StorageUnitData` 的样本存活，直到 CLEAR、覆盖或最后一个引用释放。
- GET 中，buffer 随返回给调用方的数据存活。

pool 的上限只约束空闲 buffer。仍被 RL 数据引用的 leased buffer 属于有效数据内存，不是
缓存泄漏，也不能提前复用。

## 注册与 Metadata

receive buffer 在首次分配时注册，并在 pool 和 lease 之间复用。endpoint metadata 只保留
agent 初始化时的连接信息；PREPARE 只发布当前 receive MR 的 partial agent metadata，
避免把其他已存 RL batch 的 registration 反复放入控制消息。

发送端仅在 metadata 变化时替换对应的 remote agent；旧条目移除后立即失效，只有新
metadata 加载成功才重新缓存，避免更新失败后留下与 NIXL agent 状态不一致的缓存。

GET 发送数据时，如果 frame 地址位于 leased receive MR 内，NIXL 直接使用该大 MR 中的
子区域。只有来自 checkpoint、`data_parser` 或其他外部内存的 frame 才临时注册。这样既
避免重复注册重叠区域，也符合 NIXL 使用少量大 registration、从中派生 transfer descriptor
的方式。

registration 和 transfer handle 是两类独立资源。源 MR 只能在 transfer handle 成功释放
后反注册；反注册失败时，runtime 保留 registration 和 buffer owner，直到 agent teardown。

## 生命周期

| 事件 | Pending state | Registered buffer |
| --- | --- | --- |
| PREPARE 成功 | 加入 | 从 pool 取出，或新建并注册 |
| PREPARE 失败 | 不保留 | 归还 pool |
| CANCEL before COMMIT | 删除 | 确认发送停止后归还 pool |
| COMMIT 成功 | 删除 | 租给 decoded data，最后一个 view 释放后归还 |
| Decode/store 失败 | 删除 | 已生成的 view 全部释放后归还 |
| `close()` | 全部清空 | 安全 MR 反注册；prepared MR 随 agent teardown 释放 |

PUT 被取消时，发送端先等待本地 NIXL WRITE 停止，再通知接收端回收 buffer。GET 被取消时，
客户端只在 StorageUnit 确认取消后回收 receive buffer。若确认失败，buffer 保留到 shutdown，
避免仍在进行的远端 WRITE 覆盖已复用内存；PUT 接收端未收到取消请求时，同样保留其
prepared buffer 到 shutdown。

`SimpleStorageUnit` 的单 worker 会串行处理 GET COMMIT 和 CANCEL；WRITE 未结束时 CANCEL
不能被提前确认。因此当前不需要额外的 in-flight 状态机或跨 worker 同步。

`close()` 先停止 send executor，再清理 receive pool 和 leased buffer。仍可能被远端访问的
prepared MR 会保持 owner 存活，直到 NIXL agent 销毁。

## 有意不做的设计

- 不增加 idle timeout、后台 reaper 或协议重试状态机。大 payload 的持续时间不能证明 peer
  已失效。
- 不增加通用 MR cache、区间树或公开 pool 参数。当前只复用 runtime 自己拥有且生命周期
  明确的 receive MR。
- 不为外部 source buffer 做长期注册缓存，其所有权不受 TransferQueue 控制。
- 不覆盖 accelerator memory、其他 NIXL backend 或跨版本兼容性。

参考：

- [NIXL memory section 与 teardown](https://github.com/ai-dynamo/nixl/blob/main/docs/nixl.md)
- [NIXL backend registration 与 transfer lifecycle](https://github.com/ai-dynamo/nixl/blob/main/docs/BackendGuide.md)
- [NIXL Python metadata、registration 和 transfer API](https://github.com/ai-dynamo/nixl/blob/main/src/api/python/_api.py)
- [NIXL 大 registration 与子区域 transfer 示例](https://github.com/ai-dynamo/nixl/blob/main/examples/python/expanded_two_peers.py)
