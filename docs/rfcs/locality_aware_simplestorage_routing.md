# RFC: Locality-Aware Routing for SimpleStorage

## 1. 背景与问题分析

### 1.1 当前调度机制

TransferQueue 的 SimpleStorage 使用 `global_idx % num_su` 将样本路由到 storage unit（代码位置：`simple_backend_manager.py:182 _group_by_hash`）。这个策略简单、确定性、负载均衡，但在多节点 RL 场景下产生大量跨节点流量。

### 1.2 RL 训练的实际数据流

通过对 vime 和 verl 两种 RL 框架的代码分析，实际数据流如下：

**vime（集中式 producer）**：

```text
RolloutManager（单进程 Ray actor）
  → 汇总所有 vLLM engine 的生成结果
  → 一次性 put 全部数据到 TQ
  → TQ hash routing 分散到各节点 SU

Learner（每个 DP rank 的 entry rank）
  → get_meta: controller 的 sampler 分配样本
  → get_data: 从各 SU 拉取数据
  → broadcast 给同组 TP/PP/CP rank
```

**verl（分布式 producer）**：

```text
多个 AsyncRolloutWorker（分布在不同节点）
  → 各自从 TQ 拉取 prompt
  → 各自生成 response
  → 各自 put 回 TQ

Learner
  → get_meta + get_data（同 vime）
```

### 1.3 跨节点流量的量化

以 8 节点共卡同步训练、256 样本为例：

```text
Hash routing（当前）：
  Put: 集中式 producer → 87.5% 跨节点写
  Get: 每个 DP rank 的 32 个样本分散在 8 个 SU → 87.5% 跨节点读

Stride sampling（本方案）：
  Get: 每个 DP rank 的 32 个样本全在本地 SU → 0% 跨节点读
```

### 1.4 关键发现：TQ 同时控制存储和采样

这是本方案的核心洞察。当前 TQ 架构中：

- **存储路由**：`simple_backend_manager._group_by_hash()` 决定数据存哪个 SU
- **采样分配**：`controller.get_metadata()` 调用 sampler 决定哪个 DP rank 取哪些样本

两者都在 TQ 内部，**不需要外部信息就能协调**。之前的方案（ProducerLocal/ConsumerLocal）试图通过外部 hint 来协调，但 TQ 内部已经有更好的协调手段。

### 1.5 跨节点延迟对端到端的影响

Put 和 Get 的绝对耗时很小（5-50ms），相对于 rollout generate（10-60s）和 train（1-30s）可忽略。但跨节点流量的真正影响在于：

1. **网络带宽争用**：大集群下跨节点流量与梯度同步、参数同步争用带宽
2. **SU 读取热点**：Hash routing 下一个 SU 被所有 DP rank 并发访问，吞吐成瓶颈
3. **可扩展性**：节点数从 8 扩到 32 时，hash 跨节点比例从 87.5% 升到 97%

---

## 2. 方案设计

### 2.1 核心思路：Stride Sampling

**原理**：Hash routing 是 `global_idx % num_su`，本质是 stride-1 轮转。如果 sampler 也按 stride 分配，两者天然对齐。

```text
前提条件：
  num_su == dp_size
  SU R 部署在 DP rank R 所在节点

Hash routing (global_idx % num_su):
  index 0 → SU 0 (node 0)
  index 1 → SU 1 (node 1)
  index 2 → SU 2 (node 2)
  ...
  index 8 → SU 0 (node 0)

Stride sampler (dp_rank R 取 index % dp_size == R):
  DP rank 0 → index 0, 8, 16, 24... → 全在 SU 0 (node 0) → 100% 本地
  DP rank 1 → index 1, 9, 17, 25... → 全在 SU 1 (node 1) → 100% 本地
```

**不需要 placement table，不需要 hint，不需要框架配合。**

### 2.2 GRPO 场景：Group-Block Routing + Stride Group Sampling

GRPO 中一个 prompt 生成 k 个 response，这 k 个 response 必须在同一 DP rank 计算。如果用 plain stride，group 会被拆散。

**解决**：存储路由从 `global_idx % num_su` 改为 `group_id % num_su`，sampler 以 group 为单位分配。

```text
group_size=4, num_su=8:
  group 0 (index 0,1,2,3)   → SU 0 (node 0)
  group 1 (index 4,5,6,7)   → SU 1 (node 1)
  group 2 (index 8,9,10,11) → SU 2 (node 2)
  ...
  group 8 (index 32,33,34,35) → SU 0 (node 0)

Stride group sampler (dp_rank R 取 group_id % dp_size == R):
  DP rank 0 → group 0, 8, 16... → 全在 SU 0 → 100% 本地，group 完整
  DP rank 1 → group 1, 9, 17... → 全在 SU 1 → 100% 本地，group 完整
```

### 2.3 Bias 消除：Put 时 Shuffle

#### 2.3.1 Bias 的来源

Stride sampling 的核心假设是 `global_index` 与数据内容不相关。如果 RL 框架在 put 时数据有系统性顺序（如按难度排序、按 topic 分组），stride 会引入 bias：

```text
数据按难度排序：
  index 0-3   → easy prompts
  index 4-7   → easy prompts
  index 8-11  → medium prompts
  index 12-15 → hard prompts

Stride (dp_size=4):
  DP rank 0 → group 0, 4, 8...  → 偏 easy
  DP rank 3 → group 3, 7, 11... → 偏 hard
  → 梯度方向产生系统性偏差
```

#### 2.3.2 解决方案

在 controller 的 put 路径中，分配 global_index 之前 shuffle 数据顺序：

```python
# controller.py - put 路径
def put_data(self, data, metadata, group_size=None):
    if group_size and group_size > 1:
        # GRPO: group 级别 shuffle，保持 group 内部顺序
        num_groups = len(data) // group_size
        group_perm = torch.randperm(num_groups)
        data = data.view(num_groups, group_size, ...)[group_perm].reshape(-1, ...)
    else:
        # 非 GRPO: 样本级别 shuffle
        perm = torch.randperm(len(data))
        data = data[perm]
    # 然后正常分配 global_index
```

Shuffle 后 index 与内容不相关，stride 等价于随机分区，**无 bias**。

#### 2.3.3 Shuffle 不改变存储和采样逻辑

Shuffle 只改变数据到 index 的映射，不改变 routing 和 sampling 逻辑：

```text
Shuffle 前：index 0 → prompt_1（最难的）
Shuffle 后：index 0 → random_prompt_A

无论 shuffle 与否：
  index 0 → SU 0（routing 不变）
  DP rank 0 取 index 0（sampler 不变）

变的只是 index 0 对应的内容 → 打断了 index-content 关联 → 无 bias
```

---

## 3. 方案优劣势

### 3.1 优势

**1. Get 跨节点降到 0%**

所有场景（共卡/分离 × 同步/异步 × vime/verl）的 get 跨节点都从 87.5% 降到 0%。

**2. 无算法 bias**

Put 时 shuffle 打断了 index 与内容的关联。Shuffle 后 stride 在统计上等价于随机分区：每个样本恰好被消费一次，分配到哪个 DP rank 与内容无关。

**3. 不需要框架配合**

全部在 TQ 内部完成：
- Shuffle 在 controller 的 put 路径
- Group-Block Routing 在 storage manager
- StrideSampler 是新增的 sampler 类
- SU 共置是部署文档指导

vime/verl 的 API 不变，不需要改任何框架代码。

**4. 改动极小**

```text
改动清单：
  - simple_backend_manager.py: _group_by_hash 支持 group-block 模式（~15行）
  - controller.py: put 路径加 shuffle（~20行）
  - 新增 stride_sampler.py（~80行）
  - 部署文档：说明 SU 共置建议
总计：~115 行代码
```

**5. 确定性分区，无重复消费**

每个样本恰好被消费一次。不同 DP rank 的样本集合互不重叠，无重复无遗漏。

**6. Backward compatible**

不传 `dp_size` 或 `dp_size=1` 时，StrideSampler 退化为 SequentialSampler。不传 `group_size` 时，Group-Block Routing 退化为 Hash Routing。现有行为完全不变。

### 3.2 劣势与风险

**1. 硬约束：num_su == dp_size**

这是最大的限制。如果用户有 8 个 DP rank 但只有 4 个 SU，stride 无法完美对齐。

**缓解**：支持 `num_su` 整除 `dp_size` 或 `dp_size` 整除 `num_su` 的变体。当 `dp_size = k * num_su` 时，k 个 DP rank 共享一个 SU，stride 仍可对齐。当 `num_su = k * dp_size` 时，每个 DP rank 有 k 个候选 SU，取 `index % dp_size == rank` 仍可本地命中。

**2. 不优化 put 路径**

vime 的集中式 RolloutManager put 仍然是 87.5% 跨节点。本方案只优化了 get。

**分析**：put 跨节点的绝对耗时很小（5-50ms），且是并行的（多 SU 并行写），相对 rollout generate（几十秒）可忽略。put 跨节点的根因是集中式 producer 架构，不是 routing 策略，需要框架侧改造才能解决（见第三阶段）。

**3. 确定性分区不适合所有场景**

某些 RL 算法或用户偏好要求每轮 batch 的样本组合随机变化。虽然 shuffle 后 stride 在统计上无 bias，但每个 DP rank 看到的样本子集是确定性分区的。

**缓解**：提供 `LocalityBiasedSampler`（第二阶段），支持 `locality_weight` 在 locality 和随机性之间折中。

**4. SU 故障影响扩大**

Stride 下 SU R 故障会影响 DP rank R 的 100% 样本。Hash routing 下一个 SU 故障只影响 12.5% 的样本且分散在所有 DP rank。

**缓解**：SU 故障时自动 fallback 到 hash routing，牺牲 locality 保证可用性。

**5. GRPO 需要 group_size 参数**

GRPO 场景下 shuffle 必须在 group 级别，需要知道 group_size。如果框架不传 group_size，TQ 无法知道 group 边界。

**缓解**：默认 `group_size=1`（退化为非 GRPO）。框架可通过 metadata 携带 group_size，不需要改 API。

**6. num_su != dp_size 时的退化分析**

当 `num_su` 和 `dp_size` 不整除时（如 `num_su=3, dp_size=8`），stride 无法完美对齐，部分样本仍需跨节点。

```text
num_su=3, dp_size=8:
  index 0 → SU 0, DP rank 0 取 (0%8==0) → 本地
  index 1 → SU 1, DP rank 1 取 (1%8==1) → 本地
  index 2 → SU 2, DP rank 2 取 (2%8==2) → 本地
  index 3 → SU 0, DP rank 3 取 (3%8==3) → SU 0 在 node 0, DP rank 3 在 node 3 → 跨节点
  ...
  本地命中率 ≈ num_su / dp_size = 3/8 = 37.5%（仍优于 hash 的 12.5%）
```

---

## 4. RL 场景分析

### 4.1 场景矩阵

| 场景 | Producer 分布 | Put 跨节点 | Get 跨节点（当前） | Get 跨节点（本方案） |
|------|:---:|:---:|:---:|:---:|
| vime 共卡同步 | 集中式 | 87.5% | 87.5% | **0%** |
| vime 共卡异步 | 集中式 | 87.5% | 87.5% | **0%** |
| vime 分离同步 | 集中式 | 100% | 87.5% | **0%** |
| vime 分离异步 | 集中式 | 100% | 87.5% | **0%** |
| verl 共卡同步 | 分布式 | 0% | 87.5% | **0%** |
| verl 共卡异步 | 分布式 | 0% | 87.5% | **0%** |
| verl 分离同步 | 分布式 | 100% | 87.5% | **0%** |
| verl 分离异步 | 分布式 | 100% | 87.5% | **0%** |

**注意**：vime 异步训练（`train_async.py`）不支持共卡（`assert not args.colocate`），因此 vime 共卡异步不存在。

### 4.2 各场景详细分析

#### vime 共卡同步

```text
部署：rollout 和 train 在同一组节点
Producer：RolloutManager 单进程集中 put
Consumer：每个 DP rank 的 entry rank 分布式 get

当前：put 87.5% 跨节点 + get 87.5% 跨节点
本方案：put 87.5% 跨节点（不变）+ get 0% 跨节点

收益：get 跨节点从 87.5% 降到 0%
未优化：put 跨节点（需要框架改造为分布式 put）
```

#### vime 分离同步

```text
部署：rollout 在 Node1-4，train 在 Node5-8
Producer：RolloutManager 在某个 rollout 节点集中 put
Consumer：entry rank 在 train 节点

SU 部署建议：SU 部署在 train 节点（Node5-8），不放 rollout 节点
  → put 必然跨节点（rollout → train 节点）
  → get 可以本地命中（stride 对齐）

当前：put 100% 跨节点 + get 87.5% 跨节点
本方案：put 100% 跨节点（不变）+ get 0% 跨节点
```

#### verl 共卡同步

```text
部署：rollout worker 和 train worker 在同一组节点
Producer：多个 AsyncRolloutWorker 分布式 put
Consumer：每个 worker 分布式 get

当前：put 0% 跨节点 + get 87.5% 跨节点
本方案：put 0% 跨节点（不变）+ get 0% 跨节点
→ 全链路 0% 跨节点（最理想场景）
```

#### verl 分离同步

```text
部署：rollout 在 Node1-4，train 在 Node5-8
Producer：分布式 put（rollout 节点）
Consumer：分布式 get（train 节点）

SU 部署建议：SU 部署在 train 节点
  → put 100% 跨节点（rollout → train）
  → get 0% 跨节点（stride 对齐）

如果 SU 部署在所有节点（Node1-8）：
  → put 0% 跨节点（rollout worker → 本地 SU）
  → get 0% 跨节点（stride 对齐 train 节点的 SU）
  → 但 rollout 节点的 SU 只服务 put，存储空间浪费 50%
```

#### 异步场景

异步训练中，rollout 和 train 的时间窗口重叠。本方案对异步的影响：

```text
同步训练关键路径：
  generate(30s) → put(10ms) → [barrier] → get(10ms) → train(10s)
  → get 在关键路径上，优化 get 减少尾延迟

异步训练关键路径：
  generate(N+1) 和 train(N) 重叠
  → get 的延迟被 train 时间掩盖
  → 优化 get 的价值在于减少网络带宽争用，而非减少延迟
```

异步场景下本方案的收益主要在**大规模集群的带宽争用缓解**和 **SU 读取热点消除**，而非延迟减少。

### 4.3 TP/PP 场景的影响

当训练使用 TP/PP 时：

- 每个 DP rank 只有 entry rank（tp=0, pp=0, cp=0）从 TQ 拉数据
- 然后 broadcast 给同组 TP/PP/CP rank
- **跨节点 get 发生在 entry rank 和 SU 之间**

本方案对 TP/PP 场景的影响：

```text
TP/PP 下，entry rank 代表整个 DP group 拉数据
Stride 让 entry rank 从本地 SU 拉数据 → 0% 跨节点
然后 broadcast 给同组 rank（GPU 互联，不走 TQ 网络）
→ TP/PP 不影响本方案的适用性
```

**不需要额外设计 TP/PP 感知。** Stride 的对齐粒度是 DP rank 级别，TP/PP/CP rank 在 DP group 内部，由框架的 broadcast 处理。

---

## 5. 关键修改

### 5.1 存储 Routing：支持 Group-Block 模式

**文件**：`transfer_queue/storage/managers/simple_backend_manager.py`

**当前代码**（`_group_by_hash`）：
```python
def _group_by_hash(self, global_indexes: list[int]) -> dict[str, list[int]]:
    storage_unit_keys = list(self.storage_unit_infos.keys())
    num_units = len(storage_unit_keys)
    groups: dict[str, list[int]] = defaultdict(list)
    for global_idx in global_indexes:
        groups[storage_unit_keys[global_idx % num_units]].append(global_idx)
    return dict(groups)
```

**修改后**：
```python
def _group_by_hash(self, global_indexes: list[int], group_size: int = 1) -> dict[str, list[int]]:
    storage_unit_keys = list(self.storage_unit_infos.keys())
    num_units = len(storage_unit_keys)
    groups: dict[str, list[int]] = defaultdict(list)
    for global_idx in global_indexes:
        if group_size > 1:
            # Group-block routing: same group → same SU
            group_id = global_idx // group_size
            su_idx = group_id % num_units
        else:
            # Hash routing: default behavior (backward compatible)
            su_idx = global_idx % num_units
        groups[storage_unit_keys[su_idx]].append(global_idx)
    return dict(groups)
```

**影响范围**：`put_data`、`get_data`、`clear_data` 三个路径都需要传入 `group_size`。当 `group_size=1` 时行为完全等价于当前。

### 5.2 Controller：Put 路径加 Shuffle

**文件**：`transfer_queue/controller.py`

**修改点**：在 `get_metadata(mode="insert")` 分配 global_index 之前，对数据顺序做 shuffle。

```python
# controller.py - get_metadata 的 insert 模式
if mode == "insert":
    # ... existing code ...
    
    # NEW: shuffle data order before assigning global indexes
    group_size = kwargs.get("group_size", 1)
    if group_size > 1:
        # GRPO: shuffle at group level, preserve intra-group order
        num_groups = batch_size // group_size
        perm = torch.randperm(num_groups)
        # Reorder data by group permutation
        data = self._reorder_by_group(data, perm, group_size)
    else:
        # Non-GRPO: shuffle at sample level
        perm = torch.randperm(batch_size)
        data = data[perm]
    
    # ... continue with global index allocation ...
```

**注意**：shuffle 需要 access 到实际数据。当前 `get_metadata(mode="insert")` 只分配 index，不处理数据。数据写入在 `put_data` 中。因此 shuffle 需要在 `put_data` 入口处做，或者在 client 侧做。

**更优实现**：在 `AsyncSimpleStorageManager.put_data` 入口处 shuffle，因为此时数据和 metadata 都可用：

```python
# simple_backend_manager.py - put_data
async def put_data(self, data: TensorDict, metadata: BatchMeta) -> None:
    # NEW: shuffle before routing
    group_size = metadata.group_size or 1
    if group_size > 1:
        num_groups = len(data) // group_size
        perm = torch.randperm(num_groups)
        data = self._reorder_groups(data, perm, group_size)
        # Also reorder global_indexes to match
        metadata.global_indexes = self._reorder_indexes(metadata.global_indexes, perm, group_size)
    else:
        perm = torch.randperm(len(data))
        data = data[perm]
        metadata.global_indexes = [metadata.global_indexes[i] for i in perm]
    
    # ... existing routing logic ...
```

### 5.3 新增 Sampler：StrideSampler 和 StrideGroupSampler

**文件**：`transfer_queue/sampler/stride_sampler.py`（新增）

```python
from transfer_queue.sampler import BaseSampler

class StrideSampler(BaseSampler):
    """Stride sampler that aligns with hash routing for locality.
    
    DP rank R takes indexes where index % dp_size == R.
    This aligns with hash routing (index % num_su) when num_su == dp_size
    and SU R is deployed on DP rank R's node.
    
    Requires put-time shuffle to eliminate index-content correlation bias.
    """
    
    def __init__(self):
        super().__init__()
    
    def sample(
        self,
        ready_indexes: list[int],
        batch_size: int,
        dp_rank: int = 0,
        dp_size: int = 1,
        **kwargs,
    ) -> tuple[list[int], list[int]]:
        if dp_size <= 1:
            # Single DP rank, take all
            sampled = ready_indexes[:batch_size]
            return sampled, sampled
        
        # Filter indexes belonging to this rank
        my_indexes = [idx for idx in ready_indexes if idx % dp_size == dp_rank]
        
        if len(my_indexes) < batch_size:
            return [], []
        
        sampled = my_indexes[:batch_size]
        return sampled, sampled


class StrideGroupSampler(BaseSampler):
    """Stride sampler for GRPO that preserves group integrity.
    
    DP rank R takes complete groups where group_id % dp_size == R.
    Requires group-block routing (group_id % num_su) for locality alignment.
    """
    
    def __init__(self, group_size: int = 1):
        super().__init__()
        if group_size <= 0:
            raise ValueError(f"group_size must be positive, got {group_size}")
        self.group_size = group_size
    
    def sample(
        self,
        ready_indexes: list[int],
        batch_size: int,
        dp_rank: int = 0,
        dp_size: int = 1,
        **kwargs,
    ) -> tuple[list[int], list[int]]:
        if batch_size % self.group_size != 0:
            raise ValueError(
                f"batch_size ({batch_size}) must be divisible by group_size ({self.group_size})"
            )
        
        required_groups = batch_size // self.group_size
        
        if dp_size <= 1:
            # Single DP rank, take groups sequentially
            sorted_indexes = sorted(ready_indexes)
            sampled = []
            for i in range(0, len(sorted_indexes), self.group_size):
                group = sorted_indexes[i:i + self.group_size]
                if len(group) == self.group_size and self._is_consecutive(group):
                    sampled.extend(group)
                    if len(sampled) >= batch_size:
                        break
            return sampled[:batch_size], sampled[:batch_size]
        
        # Group by group_id, filter by stride
        my_groups = {}
        for idx in ready_indexes:
            group_id = idx // self.group_size
            if group_id % dp_size == dp_rank:
                if group_id not in my_groups:
                    my_groups[group_id] = []
                my_groups[group_id].append(idx)
        
        # Select complete groups
        sampled = []
        for group_id in sorted(my_groups.keys()):
            group = my_groups[group_id]
            if len(group) == self.group_size:
                sampled.extend(sorted(group))
                if len(sampled) >= batch_size:
                    break
        
        if len(sampled) < batch_size:
            return [], []
        
        return sampled[:batch_size], sampled[:batch_size]
    
    def _is_consecutive(self, indexes: list[int]) -> bool:
        return all(indexes[j + 1] - indexes[j] == 1 for j in range(len(indexes) - 1))
```

### 5.4 不需要修改的部分

| 组件 | 是否需要修改 | 原因 |
|------|:---:|------|
| `transfer_queue/interface.py` (Client API) | 否 | `put`/`get`/`get_meta` 接口不变，`dp_rank`/`dp_size` 已在 `sampling_config` 中 |
| `transfer_queue/dataloader/streaming_dataset.py` | 否 | `default_fetch_batch_fn` 已通过 `sampling_config` 传递参数 |
| RL 框架（vime/verl）代码 | 否 | 框架已经在 `sampling_config` 中传 `dp_rank`，不需要额外配合 |
| Storage Unit 内部逻辑 | 否 | SU 只负责存取，不关心 routing |

### 5.5 配置方式

用户通过 controller 初始化时选择 sampler：

```python
from transfer_queue import TransferQueueController, StrideSampler, StrideGroupSampler

# 非 GRPO 场景
controller = TransferQueueController.remote(
    sampler=StrideSampler()
)

# GRPO 场景
controller = TransferQueueController.remote(
    sampler=StrideGroupSampler(group_size=4)
)
```

Storage manager 的 group-block routing 通过 metadata 携带 `group_size`：

```python
# 框架侧 put 时（可选，默认 group_size=1）
await client.async_put(
    data=batch,
    partition_id="train_0",
    metadata={"group_size": 4}  # GRPO 场景传入
)
```

---

## 6. Bias 分析

### 6.1 Stride Sampling 是否引入 Bias

**会，如果 index 与内容相关。** 例如数据按难度排序时，stride 会让某些 DP rank 总是拿到难的样本。

**不会，如果 index 与内容不相关。** Put 时 shuffle 打断了 index-content 关联，stride 等价于随机分区。

### 6.2 Shuffle 如何消除 Bias

```text
Shuffle 前：
  index 0-3 → prompt_1（最难的）
  index 4-7 → prompt_2（最难的）
  ...
  DP rank 0 → index 0,4,8... → 总是最难的 → bias!

Shuffle 后：
  index 0-3 → random_prompt_A
  index 4-7 → random_prompt_B
  ...
  DP rank 0 → index 0,4,8... → 随机 prompt → 无 bias
```

Shuffle 后，每个 DP rank 拿到的样本在统计上是均匀随机子集，与纯随机采样不可区分。

### 6.3 GRPO 场景的 Group 级别 Shuffle

GRPO 要求同一 prompt 的 k 个 response 保持连续。Shuffle 在 group 级别进行：

```text
原始数据（group_size=4）：
  group 0: prompt_1 的 4 个 response
  group 1: prompt_2 的 4 个 response
  group 2: prompt_3 的 4 个 response

Group 级别 shuffle 后：
  group 0: prompt_3 的 4 个 response（随机）
  group 1: prompt_1 的 4 个 response（随机）
  group 2: prompt_2 的 4 个 response（随机）

→ group 内部顺序不变（同一 prompt 的 response 仍连续）
→ group 顺序随机化（index 与 prompt 无关）
```

### 6.4 确定性分区 vs 随机采样的区别

虽然 shuffle 后 stride 在统计上无 bias，但与纯随机采样有一个区别：

```text
纯随机采样：
  step 1: DP rank 0 拿到 [A, B, C, D]
  step 2: DP rank 0 拿到 [E, F, G, H]
  → 每步的 batch 组合随机变化

Stride 采样：
  step 1: DP rank 0 拿到 [A, E, I, M]
  step 2: DP rank 0 拿到 [Q, U, Y, ...]
  → 每步的 batch 来自固定的"位置槽"
```

**对 RL 算法的影响**：

- **PPO/REINFORCE**：不影响。每个样本恰好被消费一次，梯度估计无偏。
- **GRPO**：不影响。group 完整性保持，advantage 计算正确。
- **Importance sampling**：不影响。importance weight 基于样本的 logprob 计算，与采样顺序无关。
- **Multi-epoch training**：如果一个 batch 要训练多轮（如 PPO 的 mini-batch epochs），stride 不会改变 batch 内样本的随机性（mini-batch 仍然在 batch 内随机切分）。

### 6.5 结论

**Put 时 shuffle + Stride sampling = 无 bias 的确定性分区。** 与纯随机采样在统计上等价，且保证了 locality。

---

## 7. 是否需要框架侧配合

### 7.1 第一阶段（Stride + Shuffle + Group-Block）：不需要

| 改动点 | 位置 | 是否需要框架配合 |
|--------|------|:---:|
| StrideSampler | TQ 新增 sampler | 否 |
| StrideGroupSampler | TQ 新增 sampler | 否 |
| Group-Block Routing | TQ storage manager | 否 |
| Put 时 Shuffle | TQ storage manager | 否 |
| SU 共置部署 | 部署文档 | 否（部署指导） |
| dp_rank 传递 | sampling_config | 否（vime/verl 已传） |

**vime 和 verl 都已经在 `sampling_config` 中传递 `dp_rank`**，不需要额外传任何信息。

### 7.2 唯一的可选配合：group_size 传递

GRPO 场景下，TQ 需要知道 `group_size` 才能做 group-block routing 和 group-level shuffle。

**当前 API 已支持**：通过 `put` 的 metadata 或 `sampling_config` 传递。

```python
# vime 侧（可选修改，不改 API）
await client.async_put(
    data=batch,
    partition_id=partition_id,
    metadata={"group_size": args.n_samples_per_prompt}  # 可选
)
```

如果不传 `group_size`，默认为 1，退化为非 GRPO 模式（plain stride）。功能正常，只是 GRPO 的 group 可能被拆散到不同 DP rank。

**实际上 vime 已经知道 group_size**（`args.n_samples_per_prompt`），只需在 put 时传入 metadata。

### 7.3 第三阶段（分布式 Put）：需要框架改造

如果要优化 put 跨节点（vime 的集中式 producer 问题），需要 vime 改为分布式 put。这是框架级改造，不属于 TQ 的范围。

---

## 8. 按阶段实现策略

### 8.1 第一阶段：Stride Sampling + Shuffle + Group-Block Routing

**目标**：Get 跨节点从 87.5% 降到 0%，无 bias，无框架配合。

**改动**：
1. `simple_backend_manager.py`：`_group_by_hash` 支持 `group_size` 参数
2. `simple_backend_manager.py`：`put_data` 入口加 shuffle 逻辑
3. 新增 `transfer_queue/sampler/stride_sampler.py`：`StrideSampler` + `StrideGroupSampler`
4. 部署文档：说明 SU 共置建议（`num_su == dp_size`，SU R 在 DP rank R 节点）

**验证标准**：
- 单元测试：StrideSampler 在 `num_su == dp_size` 下 100% 本地命中
- 单元测试：StrideGroupSampler 保持 group 完整性
- 单元测试：Shuffle 后 index-content 关联断裂（统计检验）
- 集成测试：8 节点共卡同步训练，get 跨节点 0%
- 回归测试：`group_size=1` 时行为等价于当前 HashRouting + SequentialSampler

**部署约束**：
- `num_su == dp_size`（或整除关系）
- SU R 部署在 DP rank R 所在节点
- 非共卡场景：SU 部署在 train 节点

### 8.2 第二阶段：LocalityBiasedSampler（可选）

**目标**：为需要随机采样的场景提供 locality-randomness tradeoff。

**改动**：新增 `transfer_queue/sampler/locality_biased_sampler.py`

```python
class LocalityBiasedSampler(BaseSampler):
    """Sampler that biases towards local samples with configurable weight.
    
    locality_weight=1.0: equivalent to StrideSampler (max locality, deterministic)
    locality_weight=0.0: equivalent to random sampling (no locality bias)
    locality_weight=0.5: half local, half random
    """
    
    def __init__(self, locality_weight: float = 0.8, group_size: int = 1):
        super().__init__()
        self.locality_weight = locality_weight
        self.group_size = group_size
    
    def sample(self, ready_indexes, batch_size, dp_rank=0, dp_size=1, **kwargs):
        # ... local/remote split + weighted sampling ...
```

**适用场景**：
- RL 算法要求每轮 batch 组合随机变化
- 用户希望保留部分采样随机性
- `num_su != dp_size` 时作为 stride 的退化替代

**验证标准**：
- `locality_weight=1.0` 时行为等价于 StrideSampler
- `locality_weight=0.0` 时行为等价于纯随机采样
- 跨节点比例与 `locality_weight` 成预期关系

### 8.3 第三阶段：分布式 Put（框架配合）

**目标**：Put 跨节点从 87.5% 降到 0%，实现全链路 0% 跨节点。

**改动**：推动 vime 改为分布式 put（类似 verl 的 AsyncRolloutWorker 模式）。

```text
当前 vime：
  RolloutManager（单进程）→ 汇总所有 engine 结果 → 集中 put

改造后：
  vLLM Engine@NodeR → generate → put 到本地 SU
  RolloutManager 只负责调度，不处理数据搬运
```

**挑战**：
1. 架构改造量大（rollout.py 重构）
2. 分布式 put 的完成通知机制
3. Group 顺序问题（各 engine 并发 put，index 分配顺序不确定）
4. 全局 shuffle 困难（每个 engine 只有局部数据）

**这不属于 TQ 的实现范围，但 TQ 可以提供支持**：
- TQ 的 `put` API 已支持分布式调用（多个 client 并发 put 到同一 partition）
- TQ 的 partition 机制已支持部分 ready（`scan_data_status` 检查每个样本的 ready 状态）
- Group 顺序问题可通过 group_key（prompt_id）解决，TQ 按 group_key 而非 index 连续性判断 group

---

## 9. 被淘汰方案及原因

### 9.1 ProducerLocal

**思路**：put 时数据放在 producer 所在节点的 SU。

**淘汰原因**：
1. vime 集中式 producer 下，全部数据堆在一个节点，get 跨节点反而恶化到 100%
2. verl 分布式 producer 下，put 已经是 0% 跨节点（各 worker put 到本地），ProducerLocal 无额外价值
3. 只优化 put，不优化 get

### 9.2 ConsumerLocal

**思路**：put 时数据放在 consumer 所在节点的 SU。

**淘汰原因**：
1. Put 时不知道 consumer（sampler 在 get_meta 时才分配样本给 DP rank）
2. 需要框架 pre-partition by DP rank before put，当前不可行
3. Stride Sampling 用更弱的前提（不需要知道 consumer）实现了相同目标（get 0% 跨节点）

### 9.3 Lazy Migration（延迟迁移）

**思路**：put → hash 存储 → get_meta 时 sampler 分配 → 迁移到 consumer 节点 → get_data 本地读。

**淘汰原因**：迁移成本等于原始跨节点 get 成本，只是转移开销，不减少总流量。

### 9.4 Learned Consumer Affinity

**思路**：第一轮 hash route + 随机采样，观察 get 模式，后续 put 按学习到的映射路由。

**淘汰原因**：每轮 rollout 的样本是全新的，学到的映射没有参考价值。样本级别没有可学习的稳定映射。

### 9.5 原方案的 Placement Table + Hint 传递

**思路**：记录每个 global_index 实际存在哪个 SU，通过 producer_hint/consumer_hint 传递 locality 信息。

**淘汰原因**：
1. Hash routing 是确定性的（`global_idx % num_su`），可计算而非需查询
2. Stride 不需要任何 hint，TQ 内部自行协调存储和采样
3. Placement table 引入额外的一致性问题和查询开销

---

## 10. 开放问题

### 10.1 num_su != dp_size 时的退化策略

当 `num_su` 和 `dp_size` 不整除时，stride 无法完美对齐。

**当前方案**：仍使用 StrideSampler，本地命中率 ≈ `num_su / dp_size`（仍优于 hash 的 `1/num_nodes`）。

**备选方案**：自动 fallback 到 `LocalityBiasedSampler`，`locality_weight = num_su / dp_size`。

### 10.2 SU 故障时的容错

Stride 下 SU R 故障会影响 DP rank R 的 100% 样本。

**方案**：SU 故障检测后，自动切换 sampler 到 `SequentialSampler`（回退到 hash routing + 顺序采样），牺牲 locality 保证可用性。SU 恢复后可手动或自动切回 StrideSampler。

### 10.3 多 epoch 训练

如果同一批数据要训练多轮（如 PPO 的 multi-epoch），stride 的确定性分区是否影响？

**分析**：不影响。Multi-epoch 通常在 batch 内做 mini-batch 随机切分，stride 只决定"哪些样本在同一个 batch"，不决定"batch 内怎么切 mini-batch"。

### 10.4 动态 DP size

训练过程中 DP size 变化（如弹性训练）时，stride 对齐会被破坏。

**方案**：DP size 变化时需要重新部署 SU（保证 `num_su == new_dp_size`），或 fallback 到 hash routing。

---

## 11. 总结

本方案的核心洞察是 **TQ 同时控制存储路由和采样分配**，通过让 sampler 适配 hash routing（而非引入外部 hint），用最小改动实现 get 跨节点 0%。

| 维度 | 第一阶段 | 第二阶段 | 第三阶段 |
|------|:---:|:---:|:---:|
| Get 跨节点 | **0%** | 0%~17.5% | 0% |
| Put 跨节点 | 不变 | 不变 | **0%** |
| 算法 bias | **无** | 有（weight<1.0） | 无 |
| 采样随机性 | 确定性分区 | 可调节 | 确定性分区 |
| 框架配合 | **否** | 否 | 是 |
| 代码改动量 | ~115行 | +~60行 | 框架侧大量 |
| 收益确定性 | **高** | 中 | 高 |

**推荐立即实施第一阶段**，验证 get 跨节点 0% 的实际收益。第二阶段按需实施。第三阶段作为长期 roadmap。
