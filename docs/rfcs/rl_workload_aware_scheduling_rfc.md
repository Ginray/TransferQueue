# RFC: RL Workload-Aware Placement and Scheduling

- **Status**: Proposed
- **Scope**: TransferQueue control-plane scheduling and SimpleStorage placement
- **Relationship to existing RFCs**: generalizes the locality discussion in v1/v2/v3

## 1. 摘要

RL 后训练中的数据调度不是单一的 locality 问题。一个可用的方案需要同时处理：

- rollout 与 learner 共卡或分离；
- 同步迭代或异步流水；
- 集中式或分布式 producer；
- GRPO group 完整性、DP/TP/PP 消费语义和 sequence-length balance；
- 长尾、背压、策略版本陈旧度、失败重试和数据生命周期；
- PUT、GET、CLEAR 对同一份数据的一致寻址。

本文提出一套统一设计：

1. **Placement 在首次写入前确定**。Controller 为 work unit 分配不可变的 `home_su`，并将 route hint 放入 metadata；PUT、GET、增量字段更新和 CLEAR 使用同一个 placement。
2. **调度单位显式化**。样本、GRPO group、trajectory 或 micro-batch 统一表示为 `WorkUnit`，group 不再通过连续 index 猜测。
3. **硬约束与软目标分离**。版本、group 完整性、消费一致性属于硬约束；locality、token balance、等待时间和负载属于软目标。
4. **同步和异步使用不同 planner**：同步模式为整个 global batch 一次性生成 `BatchPlan`；异步模式使用 node-local ready queue、bounded work stealing、aging 和 backpressure。
5. **locality 优化总生命周期成本**，而不是只优化 GET。共卡场景通常偏向 producer/consumer 共同节点；训推分离场景通常直接写入 learner 侧，接受一次不可避免的跨池 PUT，换取本地训练读取。
6. **消费使用 lease/commit**。GET_META 只预留数据，GET_DATA 成功后才提交消费；客户端失败或超时后 work unit 自动回队。

推荐首先实现确定性 placement、同步 batch planner 和异步 local-first stealing，不在第一阶段引入通用最优化求解器、自动迁移或复制。

## 2. 背景

TransferQueue 当前同时掌握两类信息：

- 数据面知道样本存在哪个 StorageUnit（SU）；
- 控制面知道哪些字段 ready、哪些 task 已消费，以及 sampler 将样本分配给谁。

这使 TransferQueue 有能力联合优化 placement 与 dispatch。但现有 hash routing 与 sampler 是相互独立的：

```text
PUT: idx % num_su -> SU
GET_META: sampler(ready_indexes) -> consumer
GET_DATA: idx % num_su -> SU
```

当每个节点部署一个 SU 时，随机 consumer 读取 hash placement 的本地命中率约为 `1/N`。单纯在 sampler 中把本地样本提前，可以缓解 GET 流量，但无法回答以下问题：

- 分离部署时，数据应该留在 rollout pool 还是提前写到 learner pool？
- 同步训练中，如何同时满足 global batch、group 完整性和 token balance？
- 异步训练中，如何避免本地队列饥饿、策略数据过旧和快 producer 挤爆存储？
- 同一 idx 分多次补字段时，谁决定它的唯一存储位置？
- consumer 在拿到 metadata 后失败，样本是否会永久丢失？

因此需要把问题从“locality-aware sampler”提升为“RL workload-aware placement and scheduling”。

## 3. 目标与非目标

### 3.1 目标

1. 在共卡、分离、同步、异步四类主要部署中提供清晰且可解释的默认策略。
2. 保留全局 work stealing，避免静态切池导致长尾节点空等。
3. 保证 PUT、GET、增量 PUT、CLEAR 和 checkpoint 使用一致的 placement。
4. 原生表达 GRPO group、DP consumer group、token cost 和 policy version。
5. 以端到端 step time、吞吐、跨节点字节和数据陈旧度为优化目标。
6. 默认行为可回退到现有 hash routing，不要求一次性重写所有 sampler。

### 3.2 非目标

1. 不在第一版实现跨 SU 自动副本一致性。
2. 不承诺所有部署下 GET 跨节点率为 0。
3. 不在控制面求解大规模精确 min-cost flow。
4. 不负责训练框架内部 TP/PP broadcast 或参数同步。
5. 不通过调度掩盖 storage capacity、网络拓扑或 worker 配置错误。

## 4. RL 工作负载模型

### 4.1 数据生命周期

典型 RL 数据会经历以下部分或全部阶段：

```text
prompt
  -> rollout(response, rollout_log_prob)
  -> reference/reward/value
  -> advantage
  -> actor update
  -> clear
```

这些阶段可能在同一组 GPU 上时分复用，也可能分布在 rollout、reward、reference、learner 多个资源池；可能由一个集中 actor 汇总后 PUT，也可能由多个 worker 独立 PUT；可能按 step 设置 barrier，也可能以持续流方式异步运行。

调度对象不能假设“一个 PUT 后紧跟一个 GET”，而应描述整个 work unit 后续可能的消费者。

### 4.2 四个核心场景

| 部署 | 执行 | 主要矛盾 | 推荐主策略 |
|---|---|---|---|
| 共卡 | 同步 | global batch 尾延迟、group/token 均衡 | 稳定 home shard + global batch plan |
| 共卡 | 异步 | producer/consumer 速率波动、热点和饥饿 | producer-affine placement + bounded stealing |
| 分离 | 同步 | rollout→learner 跨池传输不可避免，训练读取在关键路径 | consumer-home placement + global batch plan |
| 分离 | 异步 | 流水吞吐、背压、policy staleness | learner-side placement + aging/backpressure |

“共卡”和“分离”描述数据移动的物理边界；“同步”和“异步”描述调度时机。二者不能合并为同一个布尔开关。

### 4.3 Producer 形态

Producer 形态是第三个独立维度：

- **集中式 producer**：一个进程汇总全局结果。producer-local 会形成单节点热点，通常不应使用。
- **分布式 producer**：每个 rollout worker 独立写入。若其与最终 consumer 稳定共置，producer-local 才可能降低全生命周期流量。
- **多阶段 producer**：同一 idx 的不同字段由不同 stage 写入。placement 必须 first-write-wins，不能由每次 PUT 的调用者重新决定。

### 4.4 Consumer 形态

训练侧真正从 TQ 拉取数据的通常不是每张卡，而是 DP group 的 ingress rank，例如 `tp=0, pp=0, cp=0`。因此 locality 应绑定到 `ConsumerEndpoint`，而不是简单绑定 `global_rank`：

```text
ConsumerGroup
  group_id
  dp_rank
  ingress_node_id
  ingress_worker_id
  members
  capacity (samples/tokens/bytes)
```

RankAware 语义也应由 controller 一次生成 group plan，而不是依赖哪个 rank 首先请求。

## 5. 优化目标

调度目标按优先级分为三层。

### 5.1 正确性硬约束

- work unit 所需字段全部 ready；
- 未被其他有效 lease 占用或消费；
- GRPO/trajectory group 完整；
- policy/model version 满足任务要求；
- 同一个 consumer group 的 rank 获得一致的 batch plan；
- placement 的 routing epoch 与当前 metadata 一致；
- 同一 idx 的所有字段位于同一个 `home_su`。

任何 locality 优化都不得破坏这些约束。

### 5.2 活性与统计约束

- 有可用 work 时 consumer 最终能得到工作；
- remote work 不因 local-first 永久饥饿；
- 同步模式最终能构成 global batch，或显式超时失败；
- 异步模式中数据版本不超过允许的 staleness window；
- consumer 间长期吞吐和数据分布满足配置的公平性要求。

### 5.3 性能软目标

可将一次分配的边际成本抽象为：

```text
cost =
    w_net   * estimated_remote_bytes
  + w_load  * projected_consumer_load
  + w_tail  * projected_batch_tail
  + w_stale * policy_version_lag
  - w_age   * waiting_time
```

实现不要求直接计算一个精确浮点总分。第一版采用分层启发式：先满足硬约束和 aging，再在候选中优化 locality 与 load balance。

## 6. 核心抽象

### 6.1 TopologySnapshot

Topology 必须是带版本的有序快照，不能依赖 dict 插入顺序或裸 IP 比较：

```python
TopologySnapshot(
    epoch: int,
    nodes: dict[str, NodeInfo],       # stable node_id -> rack/pool/address
    storage_units: list[StorageUnitInfo],
    consumer_groups: dict[str, ConsumerGroup],
)

StorageUnitInfo(
    su_id: str,
    node_id: str,
    ordinal: int,
    capacity_bytes: int | None,
    storage_pool: str,
)
```

`node_id` 是稳定身份；IP 只用于连接。SU 的 routing 顺序通过 `ordinal` 显式表达。

### 6.2 WorkUnit

Sampler 的最小调度单位显式化：

```python
WorkUnit(
    work_id: str,
    partition_id: str,
    indexes: tuple[int, ...],
    group_id: str | None,
    estimated_bytes: int,
    estimated_tokens: int | None,
    produced_at: float,
    policy_version: int | None,
    priority: int,
)
```

- 普通 SFT/PPO 样本可以一个 index 对应一个 WorkUnit；
- GRPO 以完整 prompt group 为 WorkUnit；
- agent trajectory 可以一条 trajectory 为 WorkUnit；
- sequence-length balance 使用 `estimated_tokens`，不拆 group。

`group_id` 应由 producer/controller 明确提供，不能由“连续 N 个 index”推断。
WorkUnit 描述数据本身，不固化 `required_fields`；不同 task 的字段依赖由 ConsumerRequest 表达。

### 6.3 PlacementRecord

```python
PlacementRecord(
    partition_id: str,
    work_id: str,
    home_su: str,
    home_node: str,
    routing_epoch: int,
    state: "ALLOCATED" | "WRITTEN" | "MIGRATING",
)
```

Placement 遵循以下规则：

1. 在 insert/allocation 阶段、首次 PUT 之前生成；
2. work unit 生命周期内不可变，除非执行显式 migration transaction；
3. 后续字段增量 PUT 必须复用同一 `home_su`；
4. BatchMeta 携带 route hint，StorageManager 不自行重新决策；
5. clear partition、单样本 clear 和 checkpoint 必须同步处理 placement。

大量连续 index 可用 extent 压缩：

```text
(partition, start_idx, end_idx, routing_rule, routing_epoch)
```

只有非规则 placement 才需要逐 work unit 记录。

### 6.4 ConsumerRequest

```python
ConsumerRequest(
    task_name: str,
    required_fields: frozenset[str],
    consumer_group_id: str,
    ingress_node_id: str,
    batch_size: int,
    max_tokens: int | None,
    batch_index: int | None,
    accepted_policy_versions: VersionRange | None,
    scheduling_mode: "SYNC_GANG" | "ASYNC_STEAL",
)
```

Client 不应只注入 IP；训练框架应注册稳定的 consumer group，request 只传 group ID 和动态 batch 信息。

### 6.5 BatchPlan 与 Lease

```python
BatchPlan(
    plan_id: str,
    assignments: dict[consumer_group_id, list[work_id]],
    leases: dict[consumer_group_id, AssignmentLease],
    routing_epoch: int,
)

AssignmentLease(
    lease_id: str,
    consumer_group_id: str,
    work_ids: tuple[str, ...],
    expires_at: float,
)
```

消费状态改为：

```text
READY -> LEASED -> CONSUMED
              \-> READY       (timeout/nack)
```

- `GET_META` 为 consumer assignment 创建独立 lease，不立刻永久 mark consumed；
- `GET_DATA` 成功后 client 发送 commit；
- 读取失败发送 nack，或等待 lease 超时自动回队；
- 同一个 BatchPlan 的重复请求返回相同 assignment，保证幂等。
- 一个 consumer 的 commit/nack 不影响同一同步 plan 中其他 consumer 的 assignment。

该机制同时解决 consumer 崩溃、网络超时和 RankAware 缓存生命周期问题。

## 7. Placement Policy

### 7.1 基本原则

Placement 优化的是预计的全生命周期网络成本：

```text
placement_cost(su) =
    write_bytes * distance(producer, su)
  + sum(expected_read_bytes(stage) * distance(su, stage_consumer))
  + capacity_penalty(su)
```

不需要在第一版实现概率模型。用户通过 deployment profile 提供主要 producer pool 和 dominant consumer pool，controller 使用确定性策略。

### 7.2 三种基础策略

#### HASH

```text
home_su = ordered_sus[hash(partition_id, work_id) % num_su]
```

适用：未知拓扑、KV 通用接口、无稳定 producer-consumer 关系。它是安全 fallback。

#### CONSUMER_HOME

将 work unit 直接放到预计消费它的 learner ingress 节点。

适用：训推分离，尤其是训练 GET 位于同步关键路径或数据会在 learner 侧被多次读取。

同步模式下，placement 可以与 BatchPlan 使用相同 shard key，使一个 work unit 的所有 group 数据落在被分配 consumer 的本地 SU。

#### PRODUCER_HOME

将 work unit 放到 producer 本地 SU，但必须由 controller 在首次写入前确定，而不是每次 PUT 根据调用者 IP 临时决定。

适用：分布式 producer 与 consumer 稳定共卡，或者后续多数 stage 也在该节点执行。

不适用：集中式 producer、训推分离、producer 与 consumer 映射频繁变化。

### 7.3 AUTO 决策

AUTO 只能基于显式注册的 deployment profile，不做隐式猜测：

| 条件 | AUTO placement |
|---|---|
| rollout/learner 分离 | CONSUMER_HOME |
| 共卡且 producer→consumer 映射稳定 | PRODUCER_HOME |
| 集中式 producer | CONSUMER_HOME 或 HASH，禁止 producer hotspot |
| consumer 未注册或拓扑不完整 | HASH |
| 多下游 pool 且无 dominant consumer | HASH，等待用户显式指定 anchor pool |

### 7.4 多阶段流水的 anchor pool

当 reward、reference、learner 位于不同资源池时，默认不在每个 stage 间迁移数据。用户选择一个 `anchor_pool`：

- 大 tensor 被哪个 stage 读取最多；
- 哪个 stage 位于关键路径；
- 哪个 pool 有足够存储容量。

只有当预计未来远程读取字节大于一次迁移成本，并且 work unit 还有足够长生命周期时，才考虑显式 migration。自动 migration 不属于第一阶段。

## 8. Dispatch Policy

### 8.1 Ready index

Controller 为每个 task 维护增量索引，而不是每次扫描整个 ready pool 后构建 placement map：

```text
ready_by_node[task][node_id] -> ordered WorkUnits
ready_by_version[task][version] -> WorkUnits
aged_ready[task] -> min-heap(produced_at)
leased[lease_id] -> BatchPlan
```

生产状态从 not-ready 变为 ready 时插入；lease、commit、clear 时增量删除。已有 production-status tensor 仍是权威状态，ready index 是可重建的派生索引。

### 8.2 同步模式：Global Batch Planner

同步训练需要为整个 global batch 一次生成计划，而不是由各 rank 按请求到达顺序独立抢数据。

流程：

1. 收集满足字段、group 和 version 约束的完整 WorkUnit；
2. 达到 global batch 条件后创建 `plan_id=(partition, task, batch_index)`；
3. 按 estimated tokens/bytes 从大到小处理 WorkUnit；
4. 对每个 WorkUnit，选择边际成本最低且仍有配额的 consumer group；
5. 缓存整个 BatchPlan，各 rank/entry worker 获取自己的 slice；
6. GET 成功后按 plan commit。

第一版边际成本：

```text
marginal_cost =
    remote_bytes
  + token_balance_weight * projected_token_gap
  + sample_balance_weight * projected_sample_gap
```

对于 GRPO，分配单位始终是 group。对于 sequence-length balancing，token balance 是主目标，locality 是次目标；不能先局部取满再尝试平衡。

复杂度约为 `O(num_ready_groups * num_consumer_groups)`。global batch 通常远小于全量数据，无需引入精确求解器。

### 8.3 异步模式：Local Queue + Bounded Stealing

异步 consumer 每次 pull 一个 micro-batch：

1. 先处理超过 `max_wait_ms` 或接近 staleness 上限的 aged work；
2. 从本节点 ready queue 取满足约束的 work；
3. 本地不足时，从其他节点选择边际网络成本最低的 work stealing；
4. 创建短 lease 并立即返回；
5. 持续根据 producer/consumer 速率做 admission control。

这保持 work-conserving，同时避免纯 local-first 导致 remote 数据永久靠后。

建议初始参数：

```text
locality_target: best_effort
max_wait_ms: 2 * observed_p95_batch_service_time
lease_timeout_ms: max(3 * observed_p99_get_time, configured_minimum)
max_version_lag: algorithm-specific
```

动态观测值不足时使用显式静态配置，不自动使用不稳定估计。

### 8.4 Aging 与公平性

Locality 只能作为软优先级。满足以下任一条件的 work 进入 urgent 集合：

- 等待时间超过 `max_wait_ms`；
- policy version 即将越过允许窗口；
- 所属 producer/tenant 长期服务率低于最低 share；
- partition 即将关闭或同步 step 即将超时。

调度顺序为：

```text
hard constraints
  -> urgent/aging
  -> locality
  -> token/load balance
  -> stable original order
```

### 8.5 Backpressure

异步 RL 中，调度优化首先要避免无界队列，而不仅是减少跨节点流量。Controller 为 partition/task 暴露 credits：

```text
credit = min(
    free_storage_bytes / estimated_work_bytes,
    max_ready_work - current_ready_work,
    max_version_lag - current_version_lag_budget,
)
```

Producer 在 credit 为 0 时暂停申请新 metadata，或按框架配置丢弃过旧 rollout。建议支持：

- ready bytes high/low watermark；
- 每个 policy version 的最大未消费 work；
- producer 级 fair share；
- consumer stall detection；
- 显式 `BLOCK`、`REJECT_OLDEST_VERSION` 两种策略。

是否允许丢弃数据属于 RL 算法语义，TransferQueue 默认只阻塞，不自行丢弃。

## 9. 分场景设计

### 9.1 共卡同步

典型特点：rollout 与 learner 使用同一批节点，阶段间存在 barrier 或时分复用。

推荐：

- SU 与 learner ingress node 共置；
- 分布式 producer 使用稳定 `producer_consumer_affinity` 分配 home SU；
- 集中式 producer 不使用 producer-home，按 consumer shard 分散写入；
- controller 为整个 global batch 生成 plan；
- GRPO group/block 不拆分；
- token balance 优先于极致 locality；
- plan 构成超时后允许 remote assignment，避免 barrier 长尾。

理论上，如果 producer、consumer 和 SU 三者映射稳定，PUT/GET 都可本地；但正确目标是降低 step p99，而不是强制 0% remote。

### 9.2 共卡异步

典型特点：同节点上的 rollout 与 learner 并发或交错运行，没有严格 global barrier。

推荐：

- producer-home 作为初始 placement；
- consumer 从 node-local queue 获取；
- 本地不足时立即 steal，不能等待固定本地配额；
- 使用 aging 防止快节点数据长期滞留；
- 用 ready bytes 和 version lag 同时做背压；
- 监控节点级 producer/consumer rate，容量不足时 placement 在多个本地 SU 间做 weighted rendezvous hashing。

不建议使用严格 stride partition，因为 rollout 时延长尾会直接转化为 learner idle。

### 9.3 分离同步

典型特点：rollout pool 与 learner pool 不重叠，rollout 完成后 learner 才开始当前 step。

推荐：

- SU 部署或至少 anchor 在 learner pool；
- allocation 时按预计 learner consumer group 选择 `home_su`；
- rollout PUT 发生一次跨池传输，这是物理上不可避免的；
- learner GET 本地完成，避免在训练关键路径重复跨池拉取；
- 全局 planner 联合优化 group、token balance 和 learner locality；
- 不在 rollout pool 保留完整副本，除非后续确实复用。

把数据先写到 rollout 本地、训练时再读到 learner，不会减少总跨池字节，只会把传输推迟到更敏感的训练关键路径。

### 9.4 分离异步

典型特点：rollout(N+1) 与 train(N) 重叠，吞吐、队列长度和 policy staleness 比单次 GET 延迟更重要。

推荐：

- learner-side consumer-home placement；
- rollout 完成即流式写入对应 learner shard，不等待整 step；
- learner 使用 async stealing planner；
- 按 policy version 建 ready queue，并设置 `max_version_lag`；
- 以 learner 消费速率发放 producer credits；
- 网络繁忙时优先传输接近可组成完整 group/batch 的 work；
- 报告 generation→train 的 queueing latency 分布。

如果算法允许丢弃旧数据，丢弃单位必须是完整 WorkUnit/group，并由框架显式选择策略。

### 9.5 集中式 producer

集中式 producer 的本地节点不是合理的 storage anchor。默认行为：

- 已知 learner topology：consumer-home；
- 不知道 consumer：hash；
- 禁止 AUTO 选择 producer-home；
- PUT 并行散发到多个 SU，避免单 SU 容量和写带宽热点。

### 9.6 多阶段异步 pipeline

对于 rollout→ref/reward→learner：

- WorkUnit placement 不因每个字段 producer 改变；
- 各 stage 对同一 home SU 做增量字段更新；
- 调度 task 可有不同 ready 条件和独立 lease；
- 若多个 task 都消费同一数据，consumption status 按 task 隔离；
- anchor pool 根据最大数据字段和关键消费阶段确定。

## 10. API 草案

### 10.1 DeploymentProfile

```python
SchedulingConfig(
    mode="auto",                       # auto | sync_gang | async_steal
    placement="auto",                  # auto | hash | consumer_home | producer_home
    deployment="separated",            # colocated | separated
    producer_mode="distributed",        # centralized | distributed
    anchor_pool="learner",
    max_wait_ms=5000,
    lease_timeout_ms=30000,
    max_version_lag=None,
    enable_backpressure=True,
)
```

AUTO 只用于根据这些显式字段选择内置 policy，不从偶然的请求 IP 猜部署模式。

### 10.2 Producer 注册与 allocation

```python
producer = ProducerDescriptor(
    producer_id="rollout-3",
    node_id="node-3",
    pool="rollout",
    expected_consumer_group="learner-dp-3",
)

meta = client.allocate_meta(
    partition_id="train-42",
    work_units=work_specs,
    producer=producer,
)

# meta.route_hints: idx/work_id -> (home_su, routing_epoch)
await client.put(data, meta)
```

`put()` 不再接受一个可以覆盖 placement 的 `producer_node_ip`。

### 10.3 Consumer 请求

```python
request = ConsumerRequest(
    task_name="actor_update",
    required_fields=frozenset({"response_ids", "advantage", "old_log_prob"}),
    consumer_group_id="learner-dp-3",
    ingress_node_id="node-7",
    batch_size=8,
    max_tokens=32768,
    batch_index=step,
    accepted_policy_versions=VersionRange(step, step),
    scheduling_mode="SYNC_GANG",
)

meta = await client.get_meta(request)
data = await client.get_data(meta)
await client.commit(meta.lease_id)
```

兼容层可以在 `get_data()` 成功后自动 commit；高级用户可显式控制 commit/nack。

## 11. Controller 内部模块

建议将当前 Sampler 单接口逐步拆分为以下职责：

```text
TopologyRegistry
  -> PlacementPolicy
  -> ReadyIndex
  -> BatchPlanner / AsyncStealPlanner
  -> LeaseManager
  -> ConsumptionTracker
```

Sampler 保留算法级选择能力，但不再承担 topology 获取、placement 推断和失败恢复：

```python
class SchedulingPolicy:
    def plan(
        self,
        candidates: Sequence[WorkUnit],
        consumers: Sequence[ConsumerGroup],
        constraints: SchedulingConstraints,
    ) -> BatchPlan:
        ...
```

内置实现：

- `SequentialPolicy`
- `GroupPolicy`
- `TokenBalancedGroupPolicy`

Locality、aging、version 和 consumer load 作为 planner 的通用目标输入，不分别复制进每个 sampler。

## 12. 一致性与失败处理

### 12.1 Placement 一致性

- StorageManager 发现 route hint 缺失或 epoch 不匹配时必须报错，不能静默 fallback 到 hash；
- fallback 只能发生在首次 allocation，不能用于读取已经 placement 的数据；
- migration 使用 `PREPARE -> COPY -> SWITCH -> DELETE_OLD`，不允许直接改映射；
- 同一个 idx 的并发首次 PUT 由 controller allocation 串行确定 home SU。

### 12.2 Lease 失败

| 失败 | 行为 |
|---|---|
| client 在 GET_META 后崩溃 | lease 到期，WorkUnit 回到 ready queue |
| GET_DATA 部分失败 | nack 整个 WorkUnit/group；已读数据由 client 丢弃 |
| commit ACK 丢失 | commit 幂等，按 lease_id 重试 |
| consumer 重复请求同 batch_index | 返回相同有效 BatchPlan |
| controller 重启 | 从 checkpoint 恢复 placement/lease；过期 lease 回队 |

### 12.3 Topology 变化

- 新 topology 产生新的 routing epoch；
- 老数据继续按旧 route hint 读取；
- SU 下线且无副本时显式报告 unavailable；
- 不能通过对新 SU 数重新取模定位老数据；
- 扩缩容的数据迁移作为独立运维操作。

### 12.4 Checkpoint

Controller checkpoint 至少包括：

- topology/routing epoch；
- placement extents 和非规则 placement；
- WorkUnit/group metadata；
- consumption status；
- 尚未过期的 BatchPlan/lease，或在恢复时统一回队；
- policy version queues；
- planner 的幂等 plan cache。

Storage checkpoint 与 controller checkpoint 需要遵循现有数据面先于控制面的顺序约束。

## 13. 指标与可观测性

优化是否有效必须以字节和端到端行为验证。

### 13.1 数据面

- PUT/GET local bytes、remote bytes 和比例；
- 按 producer pool、consumer pool、SU、task 分组；
- 每个 SU 的 read/write bytes、并发数、queue time；
- route hint miss、epoch mismatch、migration bytes；
- tensor payload bytes 与控制面 bytes 分开。

### 13.2 调度面

- ready/leased/consumed WorkUnit 数和字节；
- local hit、aged pick、remote steal 比例；
- WorkUnit ready→lease、lease→commit 延迟；
- group completeness wait；
- token imbalance、samples imbalance；
- lease timeout/nack/retry；
- 每个 policy version 的 ready bytes 和消费 lag；
- producer credit、backpressure time、consumer idle time。

### 13.3 训练端

- 同步 step p50/p95/p99 和 barrier wait；
- 异步 samples/tokens per second；
- rollout→train freshness；
- 参数同步与 TQ 流量的网络争用；
- loss/reward/KL 等训练曲线，验证调度顺序影响。

不能用“跨节点样本比例下降”单独宣称优化成功。

## 14. 验证矩阵

至少覆盖：

| 维度 | 取值 |
|---|---|
| 部署 | colocated / separated |
| 执行 | sync / async |
| producer | centralized / distributed |
| sampler | sequential / GRPO group / sequence-length balanced |
| pool depth | 1x / 2x / 4x / 8x batch |
| skew | 无 / producer 速率 skew / token length skew / group late arrival |
| topology | 1 SU/node / 多 SU/node / num_su != dp_size |
| failure | GET timeout / consumer crash / controller restore / SU unavailable |

每组记录：

1. 端到端吞吐或 step time；
2. PUT、GET remote bytes；
3. consumer idle 和 barrier tail；
4. ready queue age 和 version lag；
5. SU/consumer load imbalance；
6. 正确性：不重、不漏、group 不拆、增量字段可读、clear 完整。

## 15. 分阶段实施

### Phase 0：建立基线

- 增加 PUT/GET 跨节点字节、ready age、SU load、consumer idle 指标；
- 建立四场景最小双机/多机 benchmark；
- 修正现有 RFC 中未经实测的比例结论。

成功标准：能回答瓶颈来自网络、SU、控制面扫描还是训练长尾。

### Phase 1：正确的 Placement

- 引入稳定 node ID、ordered topology 和 routing epoch；
- metadata 增加 `home_su` route hint；
- PUT/GET/CLEAR 共用 route resolver；
- first-write-wins，支持增量字段；
- placement clear/checkpoint；
- 保留 HASH 作为默认策略。

成功标准：任意 producer/consumer 进程都能一致定位数据，重启后仍正确。

### Phase 2：同步 Global Batch Planner

- 引入显式 WorkUnit/group ID；
- 为 global batch 一次生成 BatchPlan；
- 联合 group、token balance、locality；
- 以 plan_id 幂等服务各 consumer group。

成功标准：同步场景不依赖 rank 请求顺序，group 正确且 p99 不劣化。

### Phase 3：异步 Stealing 与 Backpressure

- node-local ready index；
- aging + bounded steal；
- lease/commit/nack；
- ready bytes/version lag credits。

成功标准：存在 producer skew 和 consumer crash 时仍 work-conserving，不丢数据、不无限积压。

### Phase 4：场景化 AUTO Policy

- 根据显式 DeploymentProfile 选择 producer-home、consumer-home 或 hash；
- 提供共卡/分离、同步/异步推荐模板；
- 基于 Phase 0–3 数据设置安全默认值。

成功标准：AUTO 的每个决策可解释、可观测，并能显式覆盖。

### Phase 5：可选高级能力

- extent placement 压缩；
- rack-aware distance；
- 显式 migration；
- 热数据选择性 replication/read-through cache；
- 基于历史速率的自适应权重。

这些能力只有在基础策略实测成为瓶颈后再引入。

## 16. 备选方案

### 16.1 严格 Stride Partition

优点是满足拓扑对齐时 GET 可完全本地。缺点是把 ready pool 切死，rollout 长尾会导致某些 rank 空等；DP size 和 SU topology 变化时也很脆弱。

本 RFC 将 stride 视为同步 planner 的一种低成本初始 assignment，而不是不可打破的消费边界。超时或负载失衡时允许 steal。

### 16.2 纯 Sampler Local-First

实现简单，适合作为实验基线。但它只优化 GET 次序，不解决稳定 placement、增量字段、同步 global planning、异步背压和失败恢复。

### 16.3 Producer-Local After-the-Fact Report

PUT 后上报 `idx -> node` 无法让任意 consumer 定位具体 SU，也不能保证后续字段写入同一位置。本文选择写前分配 `home_su`。

### 16.4 全量 Replication

读取本地，但存储和写流量随节点数增长。RL rollout 通常是一次性或少量消费，不适合作为默认方案。只对小而热、重复读取的数据考虑选择性复制。

### 16.5 精确 Min-Cost Flow

可以联合求解 locality 和 load balance，但控制面复杂度、延迟和可解释性较差。先采用 global batch greedy planner；只有 benchmark 证明启发式明显次优时再考虑。

### 16.6 Producer→Consumer 直传

当 producer-consumer 映射提前确定、数据不需要多 stage 更新和容错存储时，直传可能优于 TQ 中转。但这改变了 TransferQueue 的解耦与可恢复语义，属于独立数据通道，不作为本文默认路径。

## 17. 风险与开放问题

1. **Consumer 预测错误**：consumer-home placement 依赖 allocation 时知道主要 consumer。错误预测会增加 PUT/GET 总流量；应允许选择 HASH，不能静默猜测。
2. **控制面状态增长**：WorkUnit、placement 和 lease 增加状态。需要 extent 压缩、增量 ready index 和 partition 级清理。
3. **调度改变训练顺序**：完整消费不等于优化轨迹不变。需要 aging、公平性和训练曲线验证。
4. **多 task 的 anchor 选择**：同一数据被 reward、ref、actor 多次消费时，最优位置取决于字段大小和 stage topology，需要用户先显式配置。
5. **异步丢弃策略**：过旧数据是否可丢弃是算法决策。TQ 只提供版本和队列信息，默认阻塞。
6. **BatchPlan 粒度**：超大 DP world size 下 `O(groups * consumers)` 是否足够，需要 benchmark。
7. **消费 commit 时机**：自动 commit 简单但不能覆盖 client 取数后训练失败的 exactly-once 语义。默认保证“成功读取后不重复调度”，更强语义由用户显式 commit 定义。

## 18. 推荐结论

TransferQueue 的调度优化应采用以下主线：

```text
写前确定稳定 placement
  + 显式 WorkUnit/group
  + 同步 global planning / 异步 bounded stealing
  + lease/commit
  + bytes、tail、staleness、backpressure 可观测
```

Locality 是其中一个重要成本项，但不是顶层抽象：

- 共卡分布式场景优先利用 producer-consumer affinity；
- 分离场景优先把数据写到 learner/anchor pool；
- 同步场景优先保证 global batch、group 和 token balance，再优化 locality；
- 异步场景优先保证 work-conserving、aging、背压和版本新鲜度，再优化 locality；
- 任意场景都不能为了 locality 破坏稳定寻址和失败恢复。

该设计比单独增加 locality sampler 更复杂，但复杂度集中在可复用的 placement、planner 和 lease 三个模块中，能够覆盖 RL 的主要部署形态，并为后续 rack-aware、migration 和 selective replication 提供稳定基础。
