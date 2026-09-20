# NIXL-UCX SimpleStorage Host Payload Fast Path

> 状态：已实现 frame-native layout、receive lease、different-peer concurrency、deferred GET 和 quarantine。
> 发布验证尚未完成；真实 NIXL/UCX 生命周期、超过 4 GiB 传输和性能仍需通过下文的验证门槛。

## 1. 目标与冻结范围

本设计只为 RL + TransferQueue 的 SimpleStorage Host payload 增加一条 fast path：

- 用户继续调用现有 TQ/KV API，PUT、GET、CLEAR 和采样语义不变；
- ZMQ 保持控制面和默认 payload 路径；显式选择 `nixl-ucx` 后，payload 改走 NIXL-UCX；
- NIXL 只加速 Host payload movement，不成为 storage backend 或通用 transport framework；
- receiver 只有在 DMA 已停止且业务对象不再引用时才能复用，未知状态一律 quarantine。

本版必须完成六项能力：frame-native layout 原生支持单 frame 或累计 payload 超过 4 GiB、registered receive
buffer reuse + lease、减少 receive path 整包 Host copy、不同 peer/StorageUnit 并发 transfer、GET deferred
response，以及 timeout/unknown completion 下的 receiver quarantine。deferred GET 只用于避免 NIXL WRITE
阻塞 StorageUnit control worker，不用于同 peer 高并发。

超过 4 GiB 是 release gate，不提供 packed fallback。实现使用 frame-native descriptors，不额外引入 transport
chunking、reassembly 或 retry protocol。

HIXL、GPU memory、通用 transport 状态机、异步 RPC 框架、长期 external MR cache、自动 payload threshold、
透明 retry、allocator、拓扑管理和同 peer 并发不在范围内。NIXL 初始化或传输失败时明确报错，单次请求不
静默切回 ZMQ。“更快”必须由真实 RL workload 的同步 A/B 证明，不能从“用了 RDMA”或“少一次 copy”直接推断。

## 2. 架构与职责

```text
AsyncSimpleStorageManager / SimpleStorageUnit
                    |
              PayloadTransfer
             /               \
        ZMQ path       NixlPayloadTransfer
                             |
                      PREPARE / COMMIT
                             |
                        NixlRuntime
                             |
          MR / metadata / per-peer executor / Future
                             |
                         NIXL-UCX

SimpleStorage worker
  +-- ZMQMessage       -> send immediately
  `-- DeferredResponse -> continue polling -> worker sends when Future completes
```

- Manager/StorageUnit 决定传什么、存什么；Controller、sampler 和公共 API 不感知 NIXL；
- `NixlPayloadTransfer` 只实现现有控制协议与 payload fast path；
- `NixlRuntime` 是 NIXL agent API 的唯一调用边界，拥有 MR、descriptor、handle 和 buffer owner；
- 业务线程不直接管理 agent、MR、metadata 或 handle；runtime 通过 per-peer 单 worker executor 返回 Future；
- `SimpleStorageUnit` 只识别 transport-agnostic 的 deferred response，不感知 NIXL 状态；
- 每个 peer 最多一个 active transfer，不同 peer 的 handle 可以同时 active。

最后一条主要解决 Manager 并行 PUT 多个 StorageUnit 时的队头阻塞。runtime 为首次出现的 peer 懒创建
`ThreadPoolExecutor(max_workers=1)`；标准 executor 自然串行同 peer 请求，不再叠加 mutex，也不建设自定义
pending queue、wake-next、priority、fairness 或 backpressure scheduler。不同 peer 使用不同 executor。
NIXL 和 Mooncake 都采用注册内存、descriptor/batch transfer 与异步完成的数据面模式；TQ 只借用这些必要
模式，不引入 Mooncake 的 Segment allocator、中心 metadata service、拓扑管理或多协议重试。

NIXL 数据面、MR、handle、metadata、completion 和 failure lifecycle 全部限制在
`transfer_queue/storage/payload_transfer/`。目标生产代码修改范围为：

```text
transfer_queue/storage/payload_transfer/base.py
transfer_queue/storage/payload_transfer/nixl.py
transfer_queue/storage/payload_transfer/nixl_ucx_runtime.py
transfer_queue/storage/simple_storage.py  # only the generic deferred-response hook
```

`simple_storage.py` 是唯一允许的目录外生产代码修改，不得包含 NIXL-specific 状态或分支。测试和文档按需
修改；`simple_storage_manager.py`、`serial_utils.py`、`zmq_utils.py`、Controller、sampler、配置层和公共 TQ/KV
API 保持不变。如果一项优化要求继续扩大到这些模块，应先重新判断它是否属于本版本，而不是默认扩大范围。

## 3. Buffer、frame 与 registration

### 3.1 Frame-native receive layout

```text
[frame_0][frame_1]...[frame_N-1]
```

`PayloadDescriptor` 保留为现有协议中的薄 value object，只携带 `transfer_id`、`frame_sizes` 和
`payload_bytes`，负责字段校验与消息序列化，并保证 `payload_bytes == sum(frame_sizes)`。NIXL registration、
remote descriptors 和 transport metadata 不进入该对象，只由 `NixlRuntime` 管理。接收端按 `frame_sizes` 的
累计 offset 生成非空 frame 的 remote descriptors；传输完成后，以相同 offset 生成包含空 frame 的
memoryview 列表，直接交给 `decode(frames)`。

空 frame 在 `frame_sizes` 中保留为 `0`，但只为非空 frame 生成 NIXL descriptor。descriptor list 为空时自然
跳过 receive MR 分配/注册和 NIXL WRITE；decode 仍按 `frame_sizes` 收到数量和顺序一致的空 views，不增加
all-empty 状态或协议分支。

NIXL fast path 不依赖 packed wire layout，不初始化/解析 frame table，也不使用 uint32 frame offset/size。
`payload_bytes` 仅表示 `sum(frame_sizes)` 的 data bytes。ZMQ 及其他调用者继续使用现有 packed helpers，其格式
和行为不变。

offset 使用 Python 整数累计，不主动增加 transport chunking。删除 TQ 的 32-bit 限制不代表底层任意大小均
可用；NIXL、UCX、NIC、Python exporter、Host memory 和 memlock 的边界必须在目标环境验证。

### 3.2 Receive MR pool 与 lease

PREPARE 按 pool 顺序复用第一个 capacity 足够的 idle buffer；没有可用 buffer 时，分配并注册一个新的 buffer。
V1 不增加 size 排序、best-fit 或 size class。已向 peer 发布的 receive MR 不主动淘汰或反注册，直到本地
agent teardown。

COMMIT 后的所有权链为：

```text
decoded tensor/ndarray -> exporter/frame views -> lease owner token -> RegisteredReceiveBuffer
```

lease 必须能从每个 exported frame 的 backing object 强可达；最后一个 decoded 业务对象释放后，lease 才把
buffer 归还 idle pool。实现先验证原生 backing-object 生命周期，不预先增加 exporter wrapper；只有原生
buffer protocol 无法稳定保留这条链时，才在 `payload_transfer` 内增加薄 wrapper。不得修改 `serial_utils`，
也不向用户增加 `release()`、context manager 或 Future API。PUT lease 随已存数据存活，GET lease 随返回值
存活。lease 只是一枚内部 owner token，不提供显式 release/close、状态枚举、公开 refcount、callback 注册或
独立资源管理器接口；若现有 backing object + finalizer 已能表达所有权，也不要求单独定义 `ReceiveLease` 类。
finalizer 不能只挂在最初 decode 得到的 tensor/ndarray 上；例如 tensor `detach()` 后可以共享 storage 却不再
保留原 Python Tensor，必须由底层 exporter/backing object 保证派生对象存活期间 buffer 不回 pool。

无法确认远端不再写入的 buffer 不回 pool，而是由 runtime 保留到 agent teardown。

这个简单策略可能使 registered memory 随历史峰值增长，也可能因少量存活对象保留整个 receive buffer；这是
本设计为明确所有权接受的代价。是否增加容量限制由真实 RL profile 决定，不在本版预留淘汰协议。

### 3.3 发送端 source registration

1. 位于仍被 lease 的 receive MR 中的 frame 直接复用原 registration；
2. writable、C-contiguous external frame 在不与尚未释放的 source registration 重叠时直接注册；
3. readonly 或 non-contiguous frame 复制到本 transfer 的 writable contiguous buffer；
4. 需要注册的 source regions 使用 `NixlRuntime` 现有 registration 接口，registration 和 owner 只属于当前
   transfer；
5. 正常完成后清理 transfer-local registration 和 owner；异常或状态未知时保留到 agent teardown。

判断 source 是否可复用 registration 时，只线性扫描当前 `NixlRuntime` 自己仍被 lease 的 receive buffers，
根据其 `address + capacity` 判断 frame 地址范围是否完全落入其中；命中后复用该 buffer 的 registration。
不追踪外部 allocator，也不建设通用 MR lookup。

NIXL 按地址范围查找 registration，因此重叠 source 不能作为互相独立的 transfer 资源清理。runtime 记录尚未
反注册的 external source regions；遇到重叠时复制当前 frame 后单独注册，成功反注册后立即移除记录。

若现有接口自然支持，可将同一 transfer 的 regions 批量注册，但这不是架构要求或测试门槛，也不把物理不连续
frame 宣称成一个 MR。本版只持久复用 receive-side MR；external source MR 不跨 transfer 缓存。TQ 不控制
外部 tensor 的地址生命周期，因此不做 address cache、interval tree 或 allocator integration，也不为小型
readonly metadata frame 追求严格 zero-copy。

## 4. Metadata

```text
bootstrap: exchange initial full agent metadata
PREPARE:   current full agent metadata + frame descriptors
sender:    metadata unchanged -> reuse cached remote agent
           metadata changed   -> remove old remote agent -> add current full metadata
teardown:  remove remote agent
```

每次 PREPARE 都携带 `get_agent_metadata()` 返回的当前 full metadata；它包含连接信息和所有当前 registered MR，
不增加 metadata version、partial metadata、增量累积或 invalidation/retry 协议。sender 缓存每个 peer 最近一次
加载的 metadata bytes；内容未变化时直接复用，变化时先 remove 再 add。

metadata replacement 与该 peer 的 transfer 在同一个单 worker executor 中串行：只有旧 transfer 成功完成后
才能 remove remote agent，替换完成后才能提交新 WRITE。failure/timeout/unknown 后，该 peer 在当前 session
不再接受新 transfer，避免 retained handle 仍可能 active 时替换 metadata；其他 peer 不受影响。V1 只支持
`bootstrap -> transfers -> teardown` 的正常 peer session；peer 异常退出时该 peer session 失败，调用方重新
初始化 SimpleStorage/PayloadTransfer，不在 session 内自动 re-bootstrap 或恢复传输。若真实 RL profile 证明 full
metadata 交换成为瓶颈，再单独评估 partial metadata。

## 5. PUT、GET 与并发

```text
PUT:
receiver PREPARE MR
-> sender WRITE
-> WRITE 安全结束
-> COMMIT
-> decode/store

GET:
sender prepare frames
-> receiver PREPARE MR
-> COMMIT
-> sender submit WRITE
-> deferred response
-> WRITE 安全结束
-> RESPONSE
-> receiver decode
```

`NixlRuntime` 复用现有 `ThreadPoolExecutor + Future` 模型，不增加 handle completion registry、独立 poller 或
completion state machine。每个 peer 使用一个懒创建的单 worker executor；worker 在 `_send_scatter()` 内按现有
方式执行 `transfer -> check_xfer_state -> DONE`。同 peer 由标准 executor 串行，不使用额外 mutex；不同 peer
各有 executor，可以同时 active。等待期间不持有全局 runtime lock，NIXL agent API 和共享 metadata 的短操作
仍由 runtime lock 保护。

active transfer 的资源直接由 `NixlRuntime` 字段持有，或至多使用一个包含 handle、source registrations 和
owners 的简单内部记录。不建设 `TransferContext`、resource owner hierarchy、cleanup context 或 lifecycle
对象体系；现有结构能清楚表达时不新增记录类。

`NixlRuntime.send()` 返回 executor 的 `Future[None]`。PUT 等待它完成后发送 COMMIT；GET 只在
`NixlPayloadTransfer` 内用一个 callback 将其映射为 `Future[ZMQMessage]`，再生成 deferred response，不增加
completion manager。GET success response 仍是 data-ready barrier：Manager 收到它时 receiver 已经可以安全
读取。不增加 GET_ACCEPTED、GET_DONE 等控制消息。

completion 只表达 `SUCCESS` 或 `FAILURE`：success 表示 data ready，failure 表示本次请求失败且不授权
receiver 复用。不向上层暴露 transfer complete、receiver safe、handle releasable 或 cleanup complete 等
阶段。WRITE 到达 `DONE` 即确定业务 success；随后尝试一次 handle/source cleanup。cleanup 失败只把相关资源
保留到 agent teardown，不反转已经完成的业务结果；为避免 retained handle 与后续 metadata replacement 冲突，
该 peer 在当前 session 不再接受新 transfer。

### 5.1 最小 deferred-response hook

`PayloadTransfer.handle_request()` 的内部返回类型扩展为：

```python
@dataclass(frozen=True)
class DeferredResponse:
    future: Future[ZMQMessage]

handle_request(...) -> ZMQMessage | DeferredResponse | None
```

`DeferredResponse` 不携带 request context 或 transport 状态。worker 将当前 routing identity 复制为稳定的
`bytes`；completion callback 只向 thread-safe queue 写入 `(identity, future)` 并触发 poller-compatible wakeup，
绝不能操作 worker socket。worker drain queue、读取 Future 并发送 response；不能依赖现有 5 秒 poll timeout。
`Future.add_done_callback()` 每个注册只执行一次，因此不增加 local id、pending map 或 response 状态机。

NIXL 失败 Future 和意外 Future exception 都转为普通错误响应；Manager 对已开始发送 GET COMMIT 后的失败
一律按 unsafe 处理。shutdown 开始后不再接收请求，也不保证 pending deferred response 获得回复；callback
直接丢弃 completion。worker/socket 关闭后，`NixlRuntime.close()` 设置 closing event、拒绝新 submit、取消尚未
运行的 Future；active polling loop 观察到 event 后停止等待并保留 handle/MR/owner。executor 退出后先 teardown
agent，再释放 retained/quarantined owner。这个顺序不等待正常 transfer timeout；native agent teardown 能否
及时返回仍取决于 NIXL/UCX，必须在目标环境验证，不为此增加 quiesce/abort protocol。deferred response 发送
失败时 worker 退出循环并关闭自己的 socket。该 hook 不扩展为 scheduler、stream 或通用 async RPC。

## 6. 失败与清理

异常处理只遵守三条原则：

1. receiver 尚未创建或尚未向可能执行 WRITE 的 peer 暴露时，可以正常释放；
2. sender 能证明 `send()` 尚未提交或 COMMIT 尚未尝试时，可以用现有 CANCEL 释放 prepared resource；越过该边界后的
   failure/timeout/unknown 一律 quarantine receiver；
3. 无法清理的 handle、source MR 和 owner 由 `NixlRuntime` 保留到 agent teardown。

对 PUT，StorageUnit 在 READY 中发布 receive token 后即视为暴露；对 GET，Manager 开始发送携带 receive token
的 COMMIT 后即视为暴露。除“明确尚未尝试 send/COMMIT”这一处 CANCEL 边界外，runtime 不根据
submit/completion 的细分状态尝试失败后提前回收 receiver。WRITE `DONE` 后尝试一次清理，清理失败不改变
业务 success，只保留资源并拒绝该 peer 的后续 transfer；failure/timeout/unknown 时，将 handle、source
registration 和 owner 移入
`retained_resources`，不继续后台轮询或尝试 late reclaim，只在 agent teardown 时统一处理。quarantine 同样
只是 runtime 持有的 retained buffer list：buffer 不再回 idle pool，不增加状态枚举、状态迁移、reclaim queue
或独立 quarantine manager。发生这些异常的 peer 在当前 session 直接拒绝后续 transfer，不建设恢复流程。

失败 Future 只携带 transport 错误文本，不保留可能持有 native handle 的异常 traceback。runtime 在抛出错误前
先将 handle、registration 和 owner 转移到自己的 retained resources，确保 teardown 顺序不受 Future 生命周期影响。

### 6.1 PUT failure

Manager 是 sender。PUT PREPARE 发出后，如果本地失败且 `runtime.send()` 尚未成功提交，Manager 发送现有
`PUT_CANCEL`，StorageUnit 释放可能已 prepared 的 receiver。CANCEL 失败时 receiver 保留到 teardown。一旦
`send()` 返回 transfer Future，任何异常都不再发送 CANCEL，StorageUnit
将 receiver quarantine 到 agent teardown。不增加 quiesce、submit 子状态或新的协议字段。

### 6.2 GET failure

StorageUnit 是 sender。成功响应只在 WRITE `DONE`、receiver 已可安全读取后发送。GET READY 后，如果 Manager
本地 `prepare_receive()` 失败，或在明确尚未尝试发送 GET COMMIT 前失败，Manager 发送现有 `GET_CANCEL` 释放
StorageUnit 的 pending source frames，并正常释放本地未暴露的 receiver。CANCEL 失败时 pending source 留到
teardown。一旦开始发送 COMMIT 或无法确认是否送达，任何 failure/timeout/unknown 或响应丢失都不再 CANCEL，
本地 receiver quarantine 到 agent teardown。失败响应不携带额外 safety 字段。

GET PREPARE 后未收到 COMMIT 时，StorageUnit 只持有 encoded source frames，不存在 DMA。V1 不记录 timestamp、
不扫描 expiry，也不建设 reaper；orphan pending GET 直接保留到 PayloadTransfer close/teardown 统一释放。

## 7. 观测与验证

第一版只记录 `registration`、`data-transfer`、`total` 三类耗时，只维护 `registered_bytes` 和
`quarantined_bytes` 两个 runtime 指标。这些观测仅保留在 `payload_transfer` 内部供 diagnostics 和测试使用，
不接入公共 Prometheus、StorageUnit metrics 或 Controller metrics API。`encode`、`control`、`decode-store`、
active handle/lease 数量和 RSS 只在专项性能实验中临时采集，不建设持续指标。

deferred GET 返回后，`simple_storage.py` 现有 request metric 只表示 control-worker handling/dispatch latency；
完整 GET latency 由上述 payload-transfer `total` 记录。不为延长现有 metric context 再增加异步 metrics 机制。

### 7.1 单元测试

- `payload_bytes == sum(frame_sizes)`，offset 使用 Python 整数且不截断；
- frame 顺序和空 frame 与 ZMQ decode 结果一致；没有非空 descriptor 时自然跳过 MR 和 WRITE；
- NIXL 路径不调用 packed helpers，共享 `serial_utils` 行为不变；
- leased MR 通过 runtime-owned receive buffer 的线性地址范围判断复用原 registration；writable contiguous
  且不重叠的 source 直接注册，readonly/non-contiguous/重叠 source 复制；external registration 不跨 transfer 缓存；
- lease 能从 tensor/ndarray backing object 保持强可达；原 decoded object 释放但其 `detach()`/派生 view 仍存活
  时 buffer 不复用，最后一个业务引用释放后才可返回 idle pool；
- PUT 在 `send()` 前、GET 在 COMMIT 前的确定失败使用现有 CANCEL 释放 prepared resource；越过边界后不
  CANCEL；
- receiver 暴露后的 failure/timeout/unknown 一律 quarantine，相关 source resource 留到 teardown；
- WRITE `DONE` 后 handle/source cleanup 失败不反转 Future success，资源留到 teardown，且该 peer 拒绝后续
  transfer；
- GET PREPARE orphan 不按 TTL 回收并在 close 时释放；
- 不同 peer 的单 worker executor 可以同时 active；同 peer 由标准 executor 串行，没有自定义调度队列；
- 长 GET WRITE in-flight 时，同一 StorageUnit 仍可处理 PUT PREPARE、CLEAR 等控制请求；
- deferred GET 使用 copied routing identity 只发送一次 response，failure 返回普通错误响应；
- deferred GET 未完成时，closing event 能中止 runtime polling，shutdown 不等待 transfer timeout 且不保证响应；
- 保留失败 Future 到 close 之后，agent 仍先于 retained/quarantined owners 销毁；
- deferred response 发送异常时 worker 仍关闭其全部 socket；
- ZMQ backend 现有测试保持通过。

### 7.2 两节点与性能

- 使用真实 RL frame 组成持续 PUT/GET/CLEAR，校验 dtype、shape 和内容；
- 在具备足够 Host memory 和 memlock 的目标机验证单 frame 5 GiB，以及 3 GiB + 3 GiB + 2 GiB 三个 frames；
  后者验证单 frame 未溢出但累计 offset 超过 4 GiB。两项都是 release gate；普通 CI 不分配这些大 buffer，
  但覆盖同一套 descriptor/offset 算术；
- 至少两个 StorageUnit 并发传输，验证慢 peer 不阻塞快 peer；
- 保留旧 GET 返回值后继续传输，验证旧内容不被覆盖；
- 选择一个 failure/timeout 场景验证 receiver quarantine、该 peer 拒绝后续 transfer、其他 peer 不受影响，并
  验证 shutdown 有界退出；
- 对相同 workload 做同步 ZMQ/NIXL A/B，报告 `registration`、`data-transfer`、`total` 和完整 RL step 时间。

只有 UCX 日志或设备计数器证明实际选择 `rma(rc_*/...)` 等 RDMA lane，才声称 RDMA；`rma_am(tcp/...)` 只
证明功能路径可用。

## 8. 实现顺序

1. frame-native descriptors/views、empty frames、超过 4 GiB 算术和 transfer-scoped source registration；
2. receive MR pool/lease、full metadata replacement 和 lease-backed receive views；
3. per-peer single-worker executor/Future、最小 deferred-response hook，以及与这些路径
   绑定的 exposed-receiver quarantine、有界 close；
4. 单元测试、两节点大 payload/fault 验证和真实 RL A/B。

每一步独立验证；不为尚无 profile 证据的容量控制、同 peer 并发或资源淘汰预留架构。

## 参考

- [TransferQueue Issue #173](https://github.com/Ascend/TransferQueue/issues/173)
- [TransferQueue PR #163：现有 full metadata replacement 模型](https://github.com/Ascend/TransferQueue/pull/163)
- [NIXL concepts、metadata、registration 与 teardown](https://github.com/ai-dynamo/nixl/blob/main/docs/nixl.md)
- [NIXL backend 的异步 transfer 与 handle release 语义](https://github.com/ai-dynamo/nixl/blob/main/docs/BackendGuide.md)
- [NIXL 大 registration 与子区域 transfer 示例](https://github.com/ai-dynamo/nixl/blob/main/examples/python/expanded_two_peers.py)
- [Mooncake Transfer Engine：Buffer 与 BatchTransfer](https://github.com/kvcache-ai/Mooncake/blob/main/docs/source/design/transfer-engine/index.md)
