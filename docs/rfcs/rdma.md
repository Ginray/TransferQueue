# SimpleStorage Payload Transport RFC

> 状态：Host UCX Tagged MVP 已实现，默认关闭。本文只描述产品设计；机器、IP、NIC、GID、
> UCX commit、安装目录和性能命令归入独立验证记录。

## 1. 结论

SimpleStorage 保留现有 KV 接口、ZMQ 控制面和对象存储语义，为编码后的大 Host payload 增加
可选 UCX Tagged 通道：

```text
ZMQ                  request / route / descriptor / state / error / cancel
PayloadTransport     encoded large payload
SimpleStorageUnit    decode / data_parser / put / get / clear / checkpoint
```

当前实现只处理 Host memory，不使用 UCX RMA，也不支持 GPU/NPU device memory。已有验证证明
功能可行，但没有证明相对 ZMQ 存在稳定收益，因此默认配置保持关闭。

## 2. 设计原则

1. 不改变 `kv_put`、`kv_batch_get`、`kv_clear` 和 StorageManager 接口。
2. 不改变 `StorageUnitData`、哈希路由、`data_parser`、CLEAR 和 checkpoint 对象格式。
3. ZMQ 始终保留为控制面和小 payload/不可用场景的 fallback。
4. 用户只控制是否允许 payload transport，不配置 NIC、GID 或 UCX transport 参数。
5. 产品代码不识别特定机器、网卡厂商、UCX 安装目录或实验拓扑。
6. 传输开始前可以回退；开始后失败必须报错和清理，不能透明重放 PUT。
7. GPU、NPU 和 Host backend 相互隔离，通过 capability 选择，不把设备对象暴露给 KV API。

## 3. 当前代码基线

SimpleStorage 当前由 Manager 和多个 Ray StorageUnit actor 组成：

```text
AsyncSimpleStorageManager
    ├── global_idx % num_storage_units
    ├── ZMQ connection per StorageUnit
    └── async PUT / GET / CLEAR

SimpleStorageUnit
    ├── ZMQ worker
    ├── StorageUnitData
    └── optional UcxDataPlane
```

现有 `serial_utils.encode/decode` 产生一个或多个 frame。UCX 路径继续使用相同编码语义，先用
`pack_frames()` 形成连续 Host buffer，并用 64 位 offset/size 记录 frame；接收后用
`unpack_frames()` 和 `decode()` 恢复对象。`uint64` 的理论上限是 `2^64 - 1` 字节（约 16 EiB），
实际还受 64 位进程地址空间、可用内存和 UCX/操作系统接口限制；TB 级 payload 在该范围内。
当前实现仍是一个连续 buffer、一个逻辑 request，超过单 buffer 能力时需要后续引入分块协议。

## 4. 架构

### 4.1 控制面与数据面

```text
Manager                             StorageUnit
   |--------- ZMQ control ------------>|
   |<-------- READY / response ----------|
   |========== UCX payload =============>|
```

ZMQ 承载：

- sender/receiver、indexes、fields；
- transfer descriptor；
- UCP worker address 及摘要；
- PREPARE、READY、COMMIT、CANCEL 和错误。

UCX 只承载已经编码的连续 Host payload。

### 4.2 PayloadDescriptor

```python
PayloadDescriptor(
    transfer_id: str,
    tag: int,
    payload_bytes: int,
    frame_count: int,
)
```

`tag` 由 `transfer_id` 计算。descriptor 在 native allocation 前校验 identity、payload 长度、
frame 数量和实际 payload 长度。单个逻辑 payload 直接作为一个 UCX Tagged request 提交，底层
分片由 UCX 处理；TQ 不增加 1 GiB 或 64 GiB 的传输上限，也不维护独立的 chunk 状态机。

### 4.3 Worker 所有权

每个 Manager/StorageUnit 进程创建一个 UCP worker。worker、endpoint、request posting、progress、
completion 和 cancel 都由一个 owner thread 执行，避免跨线程访问 UCP 对象。Python Future 只
表示异步完成，不取得 worker 所有权。

## 5. 协议

### 5.1 PUT

```text
Manager                              StorageUnit
PUT_PREPARE(descriptor, indexes, parser)
                                     post receive
PUT_READY
UCX Tagged send
PUT_COMMIT
                                     finish receive
                                     decode -> parser -> put_data
PUT_RESPONSE
```

pending PUT 绑定 sender、descriptor、indexes 和 parser。COMMIT/CANCEL 只接受原 sender；只有
receive、decode、parser 和存储全部成功才返回成功。

### 5.2 GET

```text
Manager                              StorageUnit
GET_PREPARE(indexes, fields, address)
                                     get_data -> encode
GET_READY(descriptor)
post receive
GET_COMMIT
                                     UCX Tagged send completes
GET_RESPONSE
finish receive -> decode
```

Manager 在 COMMIT 前 post receive，避免依赖 UCX unexpected queue。`GET_RESPONSE` 表示远端 UCX
send 已完成；本地 receive Future 完成后数据才可用。

### 5.3 取消与超时

- PUT/GET cancel 使用独立 ZMQ socket，避免占用原请求 socket；
- UCX payload request 不设置固定总时限，支持 TB 级传输；
- pending 状态在完成或显式 cancel 时回收，不按 payload 大小设置短 deadline；
- request cancel 在 owner thread 上 progress 到终态；
- endpoint close 和 cancel 使用独立的有限等待，避免 shutdown 永久阻塞；
- 失败路径清理 pending receive、异步任务和 endpoint 生命周期；
- 不在数据传输中途自动切换 ZMQ，避免重复提交。

## 6. 自动发现与回退

TQ 根据节点本机网络和 `/sys/class/infiniband` 自动解析 RoCE-v2 device、port、netdev 和 GID：

1. 本机 Ray IP 与 RoCE GID 精确匹配时优先使用；
2. 控制网与 RoCE 网分离时，仅在候选唯一时选择；
3. 多候选时不猜测，关闭该进程的 UCX payload transport；
4. TQ 从当前 UCX runtime 的能力列表中选择 Host transport；进程显式设置 `UCX_TLS` 时保留
   该设置，不根据网卡厂商或设备名猜测 transport；
5. UCX extension、设备或 worker 初始化失败时，在 bootstrap 阶段使用 ZMQ。

任一 StorageUnit 未初始化 UCX 时，bootstrap 不发布 UCX endpoint 集合，所有 Manager 使用
ZMQ。个别 Manager 初始化失败时，仅该 Manager 回退。

当前只能确认 UCX provider 已创建，不能从应用指标可靠区分 RDMA/TCP/shared-memory lane；
在 route 可观测性完成前，日志中出现 `provider=ucx` 不能作为 RDMA 验收证据。

## 7. 配置与部署

唯一用户配置：

```yaml
backend:
  SimpleStorage:
    payload_transport:
      enabled: false
```

`false` 保持原 ZMQ 行为；`true` 允许 TQ 尝试 payload transport 并按能力回退。

部署契约：

- 节点基础镜像提供 UCX runtime、rdma-core 和匹配的网卡驱动；
- TQ wheel 只包含 `transfer_queue._ucx` 薄 binding，不打包 UCX 或驱动；
- wheel 不包含构建机绝对 RPATH；
- 源码构建通过 `pkg-config`、标准系统目录或构建者指定的 SDK 前缀查找 UCX；
- 正式启用前必须分别验证 Verbs、UCX Tagged 和 TQ 端到端路径。

硬件兼容补丁、非标准 UCX 构建和节点网络参数属于部署验证，不进入产品代码或用户配置。

## 8. 文件与接口

| 层 | 文件 | 当前职责 |
| --- | --- | --- |
| native | `transfer_queue/native/ucx/ucx_bindings.cpp` | Worker、Endpoint、Request、Host buffer 和 Tagged completion。 |
| discovery | `transfer_queue/storage/ucx_discovery.py` | 自动解析 RoCE-v2 device、port 和 GID。 |
| transport | `transfer_queue/storage/data_plane.py` | endpoint、descriptor、单 request、Future 和取消。 |
| Manager | `transfer_queue/storage/managers/simple_storage_manager.py` | ZMQ/UCX 选择及 PUT/GET 协议。 |
| StorageUnit | `transfer_queue/storage/simple_storage.py` | pending 状态、codec、parser、存储和取消回收。 |
| bootstrap | `transfer_queue/storage/bootstrap/simple_storage_bootstrap.py` | 收集并发布完整 endpoint 集合。 |
| codec | `transfer_queue/utils/serial_utils.py` | packed-frame 编码格式。 |
| control | `transfer_queue/utils/zmq_utils.py` | 控制消息类型。 |

## 9. GPU/NPU 扩展

`payload_transport.enabled` 表示允许内部 Transport Planner 工作，不承诺具体 provider：

| Memory kind | 候选 backend | Fallback |
| --- | --- | --- |
| Host | UCX Tagged | ZMQ |
| CUDA | UCX CUDA/GDR | D2H -> Host transport -> H2D |
| Ascend | HIXL Device Direct | CANN D2H -> Host transport -> H2D |
| CUDA 与 Ascend | 不承诺跨厂商 direct | Host staging |

后续 descriptor 需要版本化增加 memory kind 和布局信息，但设备 pointer、stream、rkey 和 native
handle 不进入公开 KV API或持久 metadata。每个 endpoint 在 bootstrap 上报 capability，传输前
按两端能力选择 backend；能力不匹配时使用 Host staging。

GPU/NPU 支持必须单独验证：

- memory registration 和设备/NIC 拓扑；
- stream/event 完成顺序；
- buffer 在 completion 前的所有权；
- D2H/H2D 与 direct 的性能差异；
- 断连、取消和回退后的资源释放。

## 10. 验收与剩余工作

当前实现已覆盖 Host PUT/GET/CLEAR、parser、并发、取消和部分故障路径。默认启用前
仍需完成：

1. UCX binding wheel 的干净镜像安装/import；
2. 实际 route、fallback、字节数、耗时和 inflight 指标；
3. `max_inflight_transfers`、inflight bytes 和 backpressure；
4. checkpoint 与进行中 transfer 的互斥；
5. 目标网络上的双向、多尺寸、并发和断连验收；
6. 上层训练的 payload、结果和训练语义等价性。

实验环境和原始结果见：

- [UCX/Verbs Host transport 验证](../rdma_phase1_host_transport_validation.md)
- [SimpleStorage 功能与性能验证](../rdma_phase2_3_validation.md)
- [HIXL 兼容性验证](../hixl_a2_rh2h_experiment_and_selection.md)

## 11. 参考资料

1. [TransferQueue 默认配置](https://gitcode.com/Ascend/TransferQueue/blob/main/transfer_queue/config.yaml)
2. [TransferQueue YuanrongStorageClient](https://gitcode.com/Ascend/TransferQueue/blob/main/transfer_queue/storage/clients/yuanrong_client.py)
3. [OpenUCX FAQ](https://openucx.readthedocs.io/en/master/faq.html)
4. [OpenUCX Features](https://openucx.readthedocs.io/en/master/ucx_features.html)
5. [OpenUCX API](https://openucx.readthedocs.io/en/master/api.html)
6. [HIXL README](https://gitcode.com/cann/hixl/blob/master/README.md)
7. [HIXL C++ 接口](https://gitcode.com/cann/hixl/blob/master/docs/zh/api/cpp/HIXL-interface.md)
8. [Mooncake Issue #719](https://github.com/kvcache-ai/Mooncake/issues/719)
9. [Mooncake PR #759](https://github.com/kvcache-ai/Mooncake/pull/759)
