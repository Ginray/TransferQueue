# RFC: Locality-Aware Soft Scheduling for SimpleStorage

- **Status**: Implemented for Phase 1
- **Scope**: TransferQueue 内部的 SimpleStorage GET 调度
- **Compatibility**: 上层 RL 框架和 Client API 不变，默认关闭

## 1. 摘要

Phase 1 只做一个最小闭环：

```text
既有 sample hash 存储路由
  + GET 本地优先
  + 本地不足立即使用远端 work
```

PUT、GET、CLEAR 都继续按 `global_idx % num_su` 路由；调度仅改变 ready
sample 的消费顺序，不改变数据 placement，也不建立逐样本目录。

## 2. 背景与收益边界

SimpleStorage 的 sample hash 能保证无状态的 PUT、GET、CLEAR 寻址一致，
但 sampler 原来只看到 `ready_indexes`，不知道样本所在节点。因此 consumer
随机取 ready sample 时，容易发生跨节点 GET。

设 N 个 consumer 节点各有一个 SU，ready pool 中有 R 个可消费 sample，单次
请求 B 个 sample，且数据均匀分布。无 locality 时，跨节点率约为：

```text
1 - 1/N
```

local-first 下单次请求的理想上限约为：

```text
max(0, 1 - R/(N*B))
```

这不是端到端承诺：consumer 节点必须有 SU，且 ready pool 要足够深。若 SU 与
consumer 完全分离，或 ready pool 长期仅一个 batch，收益接近零。

## 3. 目标与非目标

目标：

1. 不改变上层 `put/get_meta/get_data/clear` API；
2. 保持全局 ready pool 与 work stealing；
3. 对 sample 级内置 sampler 提供 work-conserving 的 local-first；
4. locality 关闭时保持原 sampler 选择顺序。

非目标：

1. PUT locality、复制、迁移或动态扩缩容；
2. 逐样本 placement directory；
3. GRPO group hash、group 对齐分配与 group locality；
4. 有界重排、aging 和防饥饿。

## 4. Phase 1 设计

### 4.1 确定性 sample hash

SimpleStorage 在初始化时得到一个有序 SU 列表。所有数据面操作使用相同顺序：

```python
target_su = ordered_su_ids[global_idx % num_su]
```

该顺序沿用既有 `storage_unit_infos` 注册顺序，不在已有数据后切换。Controller
只使用同一顺序推导 sample 所在 node，不参与 PUT、GET、CLEAR 寻址。

### 4.2 Topology 控制面

SimpleStorageManager 完成 SU 注册后，经已有 Controller ZMQ request socket 上报：

```python
{
    "ordered_su_ids": [...],
    "su_node_map": {su_id: node_ip},
}
```

这是一次内部控制面通信，不放入 `BatchMeta`，也不增加上层参数。SU 与 Client
均使用 Ray advertised IP 作为 node key，避免混用 Ray node ID 和 IP。

topology 不可用时，Controller 不做重排；sample hash 存储寻址仍正常工作。

### 4.3 Local-first

Client 在现有 `get_meta` 请求的 `sampling_config` 内自动填入自己的 node IP。
Controller 对当前 ready indexes 按 sample hash 与 topology 构造临时
`placement_map`，传给 sampler：

```python
local = [idx for idx in ready if placement_map[idx] == consumer_node_ip]
remote = [idx for idx in ready if placement_map[idx] != consumer_node_ip]
selected = (local + remote)[:batch_size]
```

local、remote 桶内均保持原 ready 顺序。本地不足时同一次请求直接从 remote
补齐：不等待、不切分 ready pool，因而保留 work-conserving 行为。

Phase 1 仅接入：

| Sampler | 行为 |
|---|---|
| `SequentialSampler` | sample 级 local-first |
| `RankAwareSampler` | 首次 assignment 时 sample 级 local-first；缓存命中保持原结果 |

其他 sampler 保持原语义。尤其 `GRPOGroupNSampler` 不在 Phase 1 重排，因为
完整 group 的 locality 需要稳定的 group placement，不能以 sample hash 假装实现。

## 5. 配置与降级

```yaml
scheduling:
  locality_aware: false
```

开启后，`tq.init()` 只为上述两个内置 sampler 打开 locality；不支持的 sampler
保留原行为并记录 warning。上层调用保持不变：

```python
meta = client.get_meta(...)
data = client.get_data(meta)
client.put(data, meta)
client.clear(meta)
```

| 情况 | 行为 |
|---|---|
| locality 关闭 | 原 sampler 行为 |
| topology 或 consumer node 缺失 | 不做 locality reorder |
| 本地候选为空 | 直接选择原顺序的 remote sample |
| 本地候选不足 | local 后立即用 remote 补齐 |

## 6. Bias

local-first 不改变完整消费时的样本集合，但会改变消费顺序。有限 step、early stop
或 index 与内容相关时，可能引入偏差；持续异步流中远端 sample 也可能等待更久。

Phase 1 默认关闭，且 local/remote 桶内保序、远端立即补齐，以限制影响。严格顺序
训练不应开启。最大重排窗口、等待时间和 bypass 计数在有实际指标后再评估。

## 7. 部署适用性

共卡、分离、同步和异步部署都可使用本机制；差别仅在 SU 是否靠近真正的 consumer。
对于 rollout 与 learner 分离的 RL 作业，应优先在 learner/Actor 消费节点部署 SU，
以优化训练关键路径的 GET。当前方案不改变 rollout 到 SU 的 PUT 流量。

`recipe/simple_use_case/relax_demo.py --locality-aware` 可验证 StreamingDataLoader
的 Phase 1 功能链路。单机运行中所有 node key 相同，预期没有跨节点性能差异；性能
验证需要多节点 placement，并在 consumer 节点部署 SU。

## 8. 后续：Phase 2 GRPO

Relax 的典型路径是 GRPO。sample hash 会将同一完整 group 分散到多个 SU；当 group
大小接近或是 SU 数量的倍数时，Phase 1 对 group GET 的收益可能很小。

若要优化 GRPO 主路径，Phase 2 需要单独实现：

1. group-aligned index allocation 与 reuse；
2. `group_id = global_idx // group_size` 的 group hash；
3. PUT、GET、CLEAR 使用相同 group 路由；
4. GRPO 仅对完整 group 做调度。

Phase 1 用于验证 topology 和 sample-level 调度闭环；Phase 2 才是对 Relax GRPO
端到端 locality 收益的前提。
