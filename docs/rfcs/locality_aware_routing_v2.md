# RFC: Locality-Aware Routing for SimpleStorage

> 状态: Draft
> 版本: v2 (从零设计)
> 日期: 2026-07-13
> 范围: TransferQueue SimpleStorage 后端
> 关联代码: `transfer_queue/storage/managers/simple_backend_manager.py`, `transfer_queue/sampler/`, `transfer_queue/controller.py`

---

## 1. 背景与动机

### 1.1 问题

TransferQueue (TQ) 在分布式 RL 训练中作为生产者-消费者之间的数据缓冲层。SimpleStorage 后端支持多 StorageUnit (SU) 部署,每个 SU 是一个独立的数据存储节点。当前跨 SU 的 GET 操作带来显著网络开销。

**实测跨节点 GET 率** (dp_size=8, num_su=8):
- 共卡同步: ~87.5%
- 分离异步: ~87.5%

GET 跨节点意味着每条样本都要走一次网络 RPC,在 10Gbps 网络下 32B 模型的 batch 数据传输可占训练 step 时间的 10-20%。

### 1.2 目标

将 GET 跨节点率从 ~87.5% 降到 **0%**,同时:
- 保持 GRPO/PPO 等算法语义不变
- 兼容 vime / verl 主流 RL 框架
- 兼容共卡/分离 × 同步/异步四种部署模式
- 不引入算法 bias

### 1.3 非目标

- 优化 PUT 跨节点 (留待后续 RFC)
- 支持 TP/PP 拓扑感知路由 (留待后续 RFC)
- 动态 SU 扩缩容 (现有 hash routing 也不支持)

---

## 2. 核心矛盾分析

### 2.1 Routing 与 Sampler 的时间轴不对称

| 维度 | Routing | Sampler |
|---|---|---|
| **决策时机** | PUT 时 (写入即落盘) | GET 时 (动态决定) |
| **可感知信息** | 仅 global_idx | ready_indexes, consumed, dp_rank |
| **可变性** | 不可变 | 每次调用重新计算 |
| **修改代价** | 需重新写入所有数据 | 改 sampler 类即可 |

**关键推论**: 要实现 GET 0% 跨节点, **必须让 sampler 适配 routing**, 而非反之。因为 routing 无法回头改,而 sampler 行为可以任意调整。

### 2.2 现有 Routing 策略

当前 SimpleStorage 仅支持 Hash Routing (见 [simple_backend_manager.py:182](../../transfer_queue/storage/managers/simple_backend_manager.py#L182)):

```python
def _group_by_hash(self, global_indexes):
    # global_idx % num_su
```

特点: 均匀分布, 但不感知 prompt group。

### 2.3 现有 Sampler 策略

TQ 内置三种 sampler (见 [sampler/](../../transfer_queue/sampler/)):

| Sampler | 行为 | dp_rank 语义 | 适用场景 |
|---|---|---|---|
| `SequentialSampler` | `ready[:N]` FIFO | 无 | SFT, 简单消费 |
| `GRPOGroupNSampler` | 取前 G 个完整连续组 | 仅 cache key | GRPO 训练 |
| `RankAwareSampler` | rank 0 取 `ready[:N]` 并 cache, 其他 rank 取相同 | cache key | TP/PP broadcast |

**vime 实际使用**: `GRPOGroupNSampler(n_samples_per_prompt=N)` (见 [vime/vime/utils/transfer_queue.py:153](file:///home/syl/code/vime/vime/utils/transfer_queue.py#L153))

### 2.4 controller 调用语义

[controller.py:1290](../../transfer_queue/controller.py#L1290):
```python
batch_global_indexes, consumed_indexes = self.sampler(
    ready_for_consume_indexes,  # 完整 ready 列表, 不按 dp_rank 过滤
    batch_size,
    **(sampling_config or {}),  # dp_rank, task_name, batch_index 透传
)
```

consumed 是 **per-task** 而非 per-rank ([controller.py:1335](../../transfer_queue/controller.py#L1335)):
```python
partition.mark_consumed(task_name, consumed_indexes)
```

**含义**: rank 0 取走的样本会被 mark_consumed, rank 1 看到的 ready_indexes 已剔除这些。这是跨 rank 不重复的保证。

---

## 3. RL 场景矩阵分析

### 3.1 四种部署模式

| 模式 | PUT 跨节点 | GET 跨节点 | 备注 |
|---|---|---|---|
| vime 共卡同步 | 0% | ~87.5% | rollout 与 train 同卡, PUT 本地 |
| vime 分离异步 | ~100% | ~87.5% | rollout 集中式, PUT 跨节点 |
| verl 共卡同步 | 0% | ~87.5% | 分布式 producer, 本地 PUT |
| verl 分离异步 | ~100% | ~87.5% | 分布式 producer, 跨节点 PUT |

### 3.2 vime 数据流

```
RolloutWorker (集中式) → PUT → TQ (多 SU) → GET → DP ranks
```

- PUT 是集中式的, 跨节点率取决于 rollout worker 是否与 SU 共位
- GET 是分布式消费, 每个 dp_rank 独立调用 get_meta

### 3.3 verl 数据流

```
多个 RolloutWorker (分布式) → PUT → TQ (多 SU) → GET → DP ranks
```

- PUT 是分布式的, 每个 rollout worker 独立 PUT
- GET 行为与 vime 相同

### 3.4 共卡/分离对 GET 的影响

**结论**: GET 跨节点率与共卡/分离**无关**。因为:
- GET 由 dp_rank 主动发起, 与 PUT 来源无关
- sampler 决定取哪些样本, 与样本来源无关

**推论**: 优化 GET 跨节点的方案对所有四种模式都有效。

---

## 4. 设计原则

### 4.1 正交解耦

将方案分解为三个正交维度:
1. **Routing 策略** (PUT 时): 决定样本落哪个 SU
2. **Sampler 策略** (GET 时): 决定 rank 取哪些样本
3. **Bias 消除** (PUT 时): 决定样本顺序

三者独立设计, 可独立开关。

### 4.2 向后兼容

- 默认行为与现有完全一致 (零回归)
- 新能力通过显式 opt-in 启用
- 不破坏现有 API

### 4.3 最小改动

- 优先扩展现有类, 而非新增类
- 框架侧改动最小化

### 4.4 算法正确性优先

- GRPO 组完整性、batch_size 整除等硬约束必须保留
- 不为性能牺牲正确性

---

## 5. 核心方案

### 5.1 方案概述

```
┌─────────────────────────────────────────────────────────┐
│  PUT 阶段                                                │
│  1. Group Routing: group_id % num_su → 样本落 SU        │
│  2. PUT-time Shuffle: 打乱 group 顺序, 消除 bias        │
├─────────────────────────────────────────────────────────┤
│  GET 阶段                                                │
│  3. GRPOGroupNSampler + stride_partition:               │
│     - 按 group_id % dp_size == dp_rank 过滤 ready       │
│     - 复用现有组完整性检查                               │
│     - 与 Group Routing 天然对齐 → 0% 跨节点 GET         │
└─────────────────────────────────────────────────────────┘
```

### 5.2 为什么有效

**Group Routing** (PUT):
```
SU 0 ← group 0, 2, 4, 6, ...   (group_id % num_su == 0)
SU 1 ← group 1, 3, 5, 7, ...   (group_id % num_su == 1)
...
```

**stride_partition** (GET, dp_size == num_su):
```
dp_rank 0 → group_id % dp_size == 0 → 全在 SU 0  ✅
dp_rank 1 → group_id % dp_size == 1 → 全在 SU 1  ✅
```

当 `dp_size == num_su` 时, sampler 的分片键与 routing 的分片键完全一致, GET 0% 跨节点。

### 5.3 为什么选择扩展 GRPOGroupNSampler 而非新建类

**GRPO 算法对 sampler 的硬约束** (缺一不可):
1. **组完整性**: 一个 prompt 的 N 个样本必须同时进一个 batch
2. **batch_size 整除**: `batch_size % n_samples_per_prompt == 0`
3. **跨 rank 不重复**: 同一 prompt 组不能被多个 rank 消费
4. **迟到样本处理**: 异步场景下跳过不完整组

现有 `GRPOGroupNSampler` 已正确实现这 4 个约束 (见 [grpo_group_n_sampler.py](../../transfer_queue/sampler/grpo_group_n_sampler.py))。新建类有重写这些逻辑的风险。

**stride 分片是正交能力**, 应作为可选开关加到现有类上, 而非替换。

---

## 6. 详细设计

### 6.1 Group Routing (PUT 策略)

#### 6.1.1 接口

在 `SimpleBackendManager` 新增 routing 策略参数:

```python
class SimpleBackendManager:
    def __init__(self, ..., routing_policy: str = "hash"):
        """
        routing_policy:
          - "hash":  idx % num_su  (默认, 向后兼容)
          - "group": group_id % num_su  (新增, 需配合 group_size)
        """
```

#### 6.1.2 实现

```python
def _group_by_routing(self, global_indexes, group_size=1):
    storage_unit_keys = list(self.storage_unit_infos.keys())
    num_units = len(storage_unit_keys)
    groups = defaultdict(list)
    
    for global_idx in global_indexes:
        if self.routing_policy == "group" and group_size > 1:
            su_idx = (global_idx // group_size) % num_units
        else:
            su_idx = global_idx % num_units  # 退化为 hash
        groups[storage_unit_keys[su_idx]].append(global_idx)
    return dict(groups)
```

#### 6.1.3 group_size 来源

从 `BatchMeta.group_size` 读取, 由 controller 在 PUT 时填充。vime 已有 `n_samples_per_prompt` 参数, controller 可直接映射。

### 6.2 PUT-time Shuffle (Bias 消除)

#### 6.2.1 问题

Group Routing 按 `group_id % num_su` 分配 SU。如果 prompt 难度按 group_id 单调分布 (例如数据集按难度排序), 则某些 SU 持续接收难题, 导致:
- 某些 dp_rank 长期训练难题 → 算法 bias
- 某些 SU 负载不均

#### 6.2.2 方案

PUT 时打乱 group 顺序:

```python
async def put_data(self, data, metadata):
    group_size = metadata.group_size or 1
    if group_size > 1:
        num_groups = len(data) // group_size
        perm = torch.randperm(num_groups)
        data = self._reorder_groups(data, perm, group_size)
        metadata.global_indexes = self._reorder_indexes(
            metadata.global_indexes, perm, group_size
        )
    # ... 后续 routing 逻辑
```

#### 6.2.3 效果

- group_id 与 prompt 内容的关联被打乱
- 每个 SU 接收的 prompt 难度分布统计均匀
- 与随机采样统计等价

### 6.3 GRPOGroupNSampler 扩展 stride_partition

#### 6.3.1 接口

```python
class GRPOGroupNSampler(BaseSampler):
    def __init__(
        self,
        n_samples_per_prompt: int = 1,
        stride_partition: bool = False,  # 新增, 默认 False
    ):
        ...
        self.stride_partition = stride_partition
```

#### 6.3.2 实现核心

```python
def sample(self, ready_indexes, batch_size, **kwargs):
    # === cache 逻辑保留不变 ===
    states = self._states.get(partition_id, {}).get(task_name, {})
    dp_rank = kwargs.get("dp_rank", None)
    batch_index = kwargs.get("batch_index", None)
    if dp_rank in states and batch_index in states[dp_rank]:
        return states[dp_rank][batch_index]

    # === batch_size 整除校验保留 ===
    if batch_size % self.n_samples_per_prompt != 0:
        raise ValueError(...)

    # === 新增: stride 分片 (在组完整性检查之前) ===
    if self.stride_partition:
        dp_size = kwargs.get("dp_size", 1)
        if dp_size > 1:
            ready_indexes = [
                idx for idx in ready_indexes
                if (idx // self.n_samples_per_prompt) % dp_size == dp_rank
            ]

    # === 以下完全复用现有逻辑 ===
    required_groups = batch_size // self.n_samples_per_prompt
    sorted_ready_indexes = sorted(ready_indexes)
    
    # 连续性检查, 取前 required_groups 个完整组
    complete_group_indices = []
    found_groups = 0
    i = 0
    while i <= len(sorted_ready_indexes) - self.n_samples_per_prompt \
          and found_groups < required_groups:
        potential_group = sorted_ready_indexes[i : i + self.n_samples_per_prompt]
        is_consecutive = all(
            potential_group[j+1] - potential_group[j] == 1
            for j in range(len(potential_group) - 1)
        )
        if is_consecutive:
            complete_group_indices.extend(potential_group)
            found_groups += 1
            i += self.n_samples_per_prompt
        else:
            i += 1

    if found_groups < required_groups:
        return [], []

    sampled_indexes = complete_group_indices
    consumed_indexes = sampled_indexes.copy()

    # === cache 写入保留 ===
    if dp_rank is not None:
        ...
    
    return sampled_indexes, consumed_indexes
```

#### 6.3.3 关键设计点

1. **stride 过滤在组完整性检查之前**: 先缩小候选集, 再做完整性检查, 逻辑正交
2. **stride_partition=False 时行为完全等同原版**: 零回归
3. **保留 cache**: TP/PP 场景下同 dp_replica_group 仍能复用结果
4. **迟到样本安全**: stride 过滤后仍走连续性检查, 不完整组会被跳过

### 6.4 controller 启动断言

新增 dp_size == num_su 的断言 (仅当 stride_partition=True 时):

```python
# controller.py, 在 get_meta 入口
if sampler.stride_partition and dp_size != num_su:
    raise RuntimeError(
        f"stride_partition requires dp_size == num_su, "
        f"got dp_size={dp_size}, num_su={num_su}. "
        f"Either disable stride_partition or align SU count with DP size."
    )
```

---

## 7. 框架配合需求

### 7.1 vime 改动

**必须改动** (1 行): sampling_config 补传 dp_size

[vime/vime/utils/transfer_queue.py:438](file:///home/syl/code/vime/vime/utils/transfer_queue.py#L438):
```python
sampling_config = {
    "dp_rank": mpu.get_data_parallel_rank(...),
    "dp_size": mpu.get_data_parallel_world_size(...),  # 新增
    "task_name": task_name,
    "batch_index": 0,
    "partition_id": ...,
}
```

**可选改动** (1 行): 启用 stride_partition

[vime/vime/utils/transfer_queue.py:153](file:///home/syl/code/vime/vime/utils/transfer_queue.py#L153):
```python
return tq.GRPOGroupNSampler(
    n_samples_per_prompt=args.n_samples_per_prompt,
    stride_partition=True,  # 新增
)
```

### 7.2 verl 改动

verl 的 TQ 集成在 `verl/experimental/` 下 (未在 main 分支默认提供)。集成方需:
1. 在 sampling_config 中传 dp_size
2. 初始化 GRPOGroupNSampler 时启用 stride_partition

### 7.3 TQ 侧改动汇总

| 文件 | 改动 | 行数 |
|---|---|---|
| `simple_backend_manager.py` | 新增 routing_policy, _group_by_routing | ~25 |
| `simple_backend_manager.py` | PUT-time shuffle | ~15 |
| `grpo_group_n_sampler.py` | 新增 stride_partition 选项 | ~15 |
| `controller.py` | 启动断言, group_size 透传 | ~10 |
| **合计** | | **~65 行** |

---

## 8. Bias 分析与消除

### 8.1 潜在 Bias 来源

| Bias 来源 | 严重性 | 是否需要消除 |
|---|---|---|
| group_id 与 prompt 难度相关 | 高 | 是 |
| stride 分片让某些 rank 长期拿难题 | 高 | 是 |
| 数据集顺序与 group_id 相关 | 中 | 是 |
| 同一 prompt 的 N 个样本顺序 | 低 | 否 (组内顺序不影响 GRPO) |

### 8.2 消除方案: PUT-time Shuffle

见 §6.2。效果:
- 每个 SU 接收的 prompt 难度分布统计均匀
- 与随机采样统计等价
- 不影响组完整性 (shuffle 整组, 不拆组)

### 8.3 残留 Bias

**无残留 bias**。原因:
- shuffle 后 group_id 与 prompt 内容无关
- stride 分片等价于随机分片
- 每个 rank 长期来看接收均匀分布的 prompt

### 8.4 验证方法

实验设计:
1. 准备难度单调递增的数据集 (如 GSM8K 按题目长度排序)
2. 启用 stride_partition + PUT-time shuffle
3. 跑 1000 steps, 记录每个 dp_rank 的平均 reward
4. 对比: 各 rank reward 方差应 < 5% (随机采样的统计涨落范围内)

---

## 9. 失败场景与容错

### 9.1 dp_size != num_su

**场景**: 用户配错, dp_size=4 但 num_su=8
**行为**: 启动断言失败, 拒绝启动
**恢复**: 用户修正配置

### 9.2 某些 group 迟到 (异步场景)

**场景**: prompt 0 的 4 个样本只 ready 了 3 个
**行为**: 
- stride 过滤后 prompt 0 的样本进入候选
- 连续性检查发现 [0,1,2,?] 不连续 → 跳过
- 取下一个完整组
**结果**: 算法正确, rank 等待更多数据

### 9.3 某些 group 永远不完整 (生成失败)

**场景**: prompt 0 的某个样本生成失败, 永远不 ready
**行为**: 
- stride 过滤后候选集包含 prompt 0
- 连续性检查永远跳过
- 候选集不足 → 返回 []
- controller 触发超时或 polling
**结果**: 算法正确, 但可能卡住。需配合上层超时机制。

### 9.4 SU 故障

**场景**: SU 1 宕机, 其上的 group 1,3,5,... 无法 GET
**行为**: 
- sampler 仍按 stride 分配 group 1,3,5 给 dp_rank 1
- GET 失败, 抛异常
**恢复**: 需要上层重启 SU 或 fallback 到 hash routing
**后续工作**: 设计 SU 故障的 routing fallback 机制 (超出本 RFC 范围)

### 9.5 dp_size 动态变化

**场景**: 训练中动态调整 DP size
**行为**: 已落盘数据的 routing 不可变, 新 dp_size 与旧 routing 错配
**结果**: 跨节点 GET 率回升, 但不破坏算法正确性
**建议**: 文档约束 dp_size 在训练中不可变 (现有 hash routing 也有此约束)

---

## 10. 实现阶段

### 阶段 1: 核心能力 (本 RFC 范围)

**目标**: GET 0% 跨节点, 兼容 vime/verl

**交付**:
- Group Routing (`routing_policy="group"`)
- GRPOGroupNSampler + stride_partition
- PUT-time shuffle
- controller 启动断言
- vime 1 行 sampling_config 改动

**验证**:
- 单元测试: stride 分片正确性, 组完整性保留
- 集成测试: vime + GRPO, GET 跨节点率 = 0%
- Bias 测试: 难度排序数据集, 各 rank reward 方差 < 5%

### 阶段 2: Locality-Biased Sampling (后续 RFC)

**目标**: 为 SequentialSampler/PPO 场景提供 partial locality

**思路**: 
- 不强制 stride 分片
- sampler 优先从本地 SU 取, 不足时跨节点
- tradeoff: 跨节点率 10-30% (优于 87.5%), 但保留 FIFO 语义

### 阶段 3: PUT 0% 跨节点 (后续 RFC)

**目标**: 全链路 0% 跨节点

**思路**:
- 分布式 producer 场景下, 每个 producer PUT 到本地 SU
- 需要框架侧告知 producer 自己的 SU 归属
- 与本 RFC 正交, 可叠加

---

## 11. 验证方案

### 11.1 单元测试

```python
def test_stride_partition_basic():
    """stride 分片正确性"""
    sampler = GRPOGroupNSampler(n_samples_per_prompt=4, stride_partition=True)
    ready = [0,1,2,3, 4,5,6,7, 8,9,10,11, 12,13,14,15]
    
    # dp_rank 0, dp_size 2 → group 0, 2 → [0,1,2,3, 8,9,10,11]
    sampled, _ = sampler.sample(ready, 8, dp_rank=0, dp_size=2, 
                                 task_name="t", partition_id="p", batch_index=0)
    assert sorted(sampled) == [0,1,2,3, 8,9,10,11]

def test_stride_partition_preserves_group_integrity():
    """stride 模式下组完整性检查仍生效"""
    sampler = GRPOGroupNSampler(n_samples_per_prompt=4, stride_partition=True)
    # prompt 0 缺样本 3
    ready = [0,1,2, 4,5,6,7, 8,9,10,11]
    
    sampled, _ = sampler.sample(ready, 8, dp_rank=0, dp_size=2,
                                 task_name="t", partition_id="p", batch_index=0)
    # 应跳过不完整的 prompt 0, 取 prompt 2
    assert sorted(sampled) == [8,9,10,11] or sampled == []

def test_stride_partition_disabled_is_backward_compatible():
    """stride_partition=False 行为等同原版"""
    sampler_old = GRPOGroupNSampler(n_samples_per_prompt=4)
    sampler_new = GRPOGroupNSampler(n_samples_per_prompt=4, stride_partition=False)
    ready = list(range(16))
    
    s1, _ = sampler_old.sample(ready, 8, task_name="t", partition_id="p", batch_index=0)
    s2, _ = sampler_new.sample(ready, 8, task_name="t", partition_id="p", batch_index=0)
    assert s1 == s2
```

### 11.2 集成测试

- 部署 8 SU, dp_size=8
- vime + GRPO, Qwen2.5-0.5B, GSM8K
- 监控 GET 跨节点率 (TQ 内置 metric)
- 预期: 跨节点率 = 0%

### 11.3 Bias 测试

- GSM8K 按题目长度降序排序 (人为制造难度单调)
- 启用 PUT-time shuffle
- 跑 1000 steps
- 记录每个 dp_rank 的 mean reward
- 预期: 各 rank reward 方差 / 均值 < 5%

---

## 12. 开放问题

### 12.1 dp_size == num_su 的硬约束是否过于严格

**现状**: 必须 dp_size == num_su
**问题**: 用户可能想 num_su > dp_size (例如 SU 部署在所有 GPU, 但 DP 只用部分)
**思路**: 允许 num_su = k * dp_size, 每个 rank 对应 k 个 SU。但 sampler 需感知多个 SU, 复杂度上升。
**决定**: 阶段 1 严格约束, 阶段 2 视需求放松

### 12.2 SequentialSampler 场景如何优化

**现状**: SequentialSampler 无 stride, GET 跨节点 ~87.5%
**问题**: SFT/PPO without group 场景仍有跨节点开销
**思路**: 新增 SequentialStrideSampler, 但需评估 FIFO 语义是否可放弃
**决定**: 阶段 2 处理

### 12.3 PUT-time shuffle 的随机种子

**现状**: 每次 PUT 用 torch.randperm, 种子未固定
**问题**: 可复现性实验需要固定种子
**思路**: 从 BatchMeta 传入种子, 或基于 rollout_id 派生
**决定**: 阶段 1 不固定 (默认行为), 阶段 2 加可配置种子

### 12.4 SU 故障的 routing fallback

**现状**: SU 故障时 stride 分片会持续失败
**思路**: 检测 SU 不可用, fallback 到 hash routing (跨节点但可用)
**决定**: 后续 RFC

---

## 13. 替代方案与淘汰理由

### 13.1 让 routing 适配 sampler (反向思路)

**思路**: 根据 sampler 类型动态调整 routing
**淘汰理由**: routing 在 PUT 时决策, sampler 在 GET 时决策。PUT 时不知道 GET 会取哪些样本, 信息不对称, 无法对齐。详见 §2.1。

### 13.2 新建 StrideGroupSampler 独立类

**思路**: 不依赖 GRPOGroupNSampler, 新写一个 stride sampler
**淘汰理由**: 
- 丢失组完整性检查 → GRPO 算法风险
- 丢失 batch_size 校验
- 丢失 cache 机制 (TP/PP 场景需要)
- 重复造轮子, 维护成本高

### 13.3 LocalityBiasedSampler (优先本地 SU)

**思路**: sampler 优先从本地 SU 取, 不足时跨节点
**淘汰理由**: 
- 跨节点率仍有 10-30%, 不如 stride 的 0%
- sampler 需感知 SU 拓扑, 复杂度高
- 保留: 作为阶段 2 的 SequentialSampler 优化方案

### 13.4 框架侧显式注册 topology

**思路**: 让 vime/verl 显式告知 TQ 每个 rank 对应哪个 SU
**淘汰理由**: 
- 框架侧改动大, 侵入性强
- 本方案通过 stride + group routing 隐式对齐, 框架只需传 dp_size
- 保留: TP/PP 场景可能需要 (后续 RFC)

---

## 14. 决策记录

| 决策 | 选择 | 理由 |
|---|---|---|
| Routing 策略 | Group Routing | 感知 group, 与 stride 对齐 |
| Sampler 改造 | 扩展 GRPOGroupNSampler | 保留组完整性, 向后兼容 |
| Bias 消除 | PUT-time shuffle | 统计等价, 不破坏组 |
| dp_size 约束 | 必须 == num_su | 简化设计, 阶段 1 足够 |
| 框架配合 | vime 改 2 行 | 最小侵入 |
| 实现策略 | 单阶段交付 | ~65 行, 风险可控 |

---

## 15. 附录

### 15.1 场景矩阵验证

| 场景 | PUT 跨节点 | GET 跨节点 (优化前) | GET 跨节点 (优化后) |
|---|---|---|---|
| vime 共卡同步 | 0% | 87.5% | **0%** |
| vime 分离异步 | ~100% | 87.5% | **0%** |
| verl 共卡同步 | 0% | 87.5% | **0%** |
| verl 分离异步 | ~100% | 87.5% | **0%** |

### 15.2 代码改动清单

| 文件 | 函数/类 | 改动类型 | 行数 |
|---|---|---|---|
| `simple_backend_manager.py` | `__init__` | 新增 routing_policy 参数 | +3 |
| `simple_backend_manager.py` | `_group_by_routing` | 新增方法 | +20 |
| `simple_backend_manager.py` | `put_data` | 新增 shuffle + 调用新 routing | +15 |
| `grpo_group_n_sampler.py` | `__init__` | 新增 stride_partition 参数 | +2 |
| `grpo_group_n_sampler.py` | `sample` | 新增 stride 过滤分支 | +10 |
| `controller.py` | `get_meta` | 新增 dp_size==num_su 断言 | +5 |
| `controller.py` | `put_data` | 透传 group_size | +5 |
| `interface.py` | sampler 导出 | 无需改动 | 0 |
| **合计** | | | **~65** |

### 15.3 vime 改动清单

| 文件 | 行 | 改动 |
|---|---|---|
| `vime/utils/transfer_queue.py` | 438 | sampling_config 加 dp_size |
| `vime/utils/transfer_queue.py` | 153 | GRPOGroupNSampler 加 stride_partition=True |
| **合计** | | **2 行** |

### 15.4 术语表

| 术语 | 含义 |
|---|---|
| SU | Storage Unit, SimpleStorage 的存储节点 |
| DP | Data Parallel |
| dp_rank | Data Parallel rank ID |
| dp_size | Data Parallel world size |
| group | GRPO 中一个 prompt 的 N 个样本 |
| group_id | `global_idx // n_samples_per_prompt` |
| stride_partition | sampler 按 group_id % dp_size 分片 |
| Group Routing | routing 按 group_id % num_su 分配 SU |
| PUT-time shuffle | PUT 时打乱 group 顺序消除 bias |
