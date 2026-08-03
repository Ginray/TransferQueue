# RFC: Locality-Aware Routing for SimpleStorage


## 1. 背景与问题

SimpleStorage 默认使用 hash routing(`global_idx % num_su`)。N 节点部署下每个 batch 的 GET 跨节点率约为 `(N-1)/N`:

| N | 跨节点率 |
|---|---|
| 4 | 75% |
| 8 | 87.5% |
| 16 | 93.75% |

内置 sampler(`SequentialSampler`、`GRPOGroupNSampler`、`RankAwareSampler`)均不感知 locality:controller 调 sampler 时只传 `ready_indexes`(纯 idx 列表),sampler 不知道每个 idx 落在哪个 SU,无法做本地优先决策。

**原因**:routing 在 PUT 时决定数据落点,但这个信息在 GET 时丢失了。

## 2. 方案总览

**routing 侧记录 placement,sampler 侧自适应本地优先**。两端共享同一份 placement 信息,联合优化。GET 与 PUT 两阶段正交,可独立启用。

```
PUT (producer-local routing, 可选)
  → 路由数据到 producer 本地 SU,记录 _idx_to_su[idx] = su_id
  → notify_data_update 携带 idx_to_node_ip
  → controller 缓存到 _idx_placement[partition_id][idx]

TOPOLOGY REPORT (按需)
  → 仅 locality_aware=True 时,storage_manager 上报 su_node_map
  → controller 缓存到 _su_node_map

GET
  → controller.get_metadata 构建 placement_map(优先用上报值,缺失时用 hash 规则补齐):
      1. _idx_placement[partition_id]  ← producer-local 实际位置(若已上报)
      2. idx % num_su → _su_node_map[su_id]  ← hash 规则自算
  → sampler.sample(consumer_node_ip, placement_map) 在自身语义约束内做本地优先
```

## 3. 设计决策

### 3.1 Locality 作为通用能力下沉,而非独立 Sampler

把 locality 下沉为 `BaseSampler` 的通用能力,每个 sampler 在**自身语义约束内**做本地优先:

| Sampler | 语义约束 | Locality 优化方式 |
|---|---|---|
| `SequentialSampler` | 顺序 | local 子集 + remote 子集,各自保持原序,local 优先 |
| `GRPOGroupNSampler` | group 完整性 | group 级重排,本地样本多的 group 优先 |
| `RankAwareSampler` | rank 一致性 | 首个 rank 采样时本地优先 reorder |

独立 `LocalityBiasedSampler` 不可行:它会替代 sampler 的语义契约(顺序/group 完整性/rank 一致性),用户无法同时满足两个约束。Locality 应是正交维度。

### 3.2 显式开关,默认关闭

每个 sampler 构造时接受 `locality_aware: bool = False`。默认关闭理由:locality 优化会改变样本顺序,对依赖严格顺序的训练(如 curriculum learning)有副作用。

触发条件:`locality_aware` opt-in + `consumer_node_ip` 存在 + `placement_map` 非空。任一不满足,走原逻辑。

### 3.3 Placement 信息来源

| 优先级 | 来源 | 适用场景 | 开销 |
|---|---|---|---|
| 1 | `_idx_placement`(PUT 时上报) | producer-local routing | 每次 PUT 额外传 `{idx: node_ip}` |
| 2 | `idx % num_su → _su_node_map`(controller 自算) | hash routing | 零开销 |

hash routing 下 placement 确定性,controller 自算即可;producer-local routing 下 placement 取决于 producer 位置,必须显式上报。

### 3.4 Topology Report 按需触发

`_su_node_map` 通过独立 `TOPOLOGY_REPORT` 消息上报,而非塞进 handshake body。handshake ACK 携带 `locality_aware` 标志,仅开启时才触发上报。复用 handshake 同步 ZMQ context,避免异步 context 返回 coroutine 的问题。Best-effort,失败不影响存储。

### 3.5 Producer-Local 多本地 SU 均衡

单节点部署多 SU 场景下,naive producer-local 会把所有样本路由到第一个本地 SU,导致单 SU 容量超限。`_group_by_producer_local` 三段策略:

| 场景 | 路由策略 |
|---|---|
| 无 `producer_node_ip` 或本节点无 SU | fallback 到全局 hash routing |
| 本节点仅 1 个 SU | 全部路由到该 SU |
| 本节点有多个 SU | `global_idx % len(local_sus)` 在本地 SU 间 hash 均衡 |

### 3.6 GRPO 的 group 级重排

GRPO 的硬约束是 group 完整性,重排最小单位是 group。实现从"边扫边取"改为"先收集所有 complete group,再按 group 内本地样本数稳定降序排,最后选前 N 个"。稳定排序保证 locality 相同时保持原扫描顺序(向后兼容)。

## 4. Bias 分析

**结论:在全数据消费的训练范式下不引入 bias。**

### 4.1 无 bias 的保证

| 维度 | 分析 |
|---|---|
| Placement 与内容相关性 | hash routing:`idx % num_su` 与内容无关;producer-local:若 producer 调度与数据内容无关(如 random shuffle 后均匀分发),placement 也与内容无关 |
| 采样覆盖率 | sampler 不切分 ready 池,保留 work stealing。本地不够时从远端补,最终全池数据都被消费 |
| 采样顺序 | locality 改变 batch 内样本顺序,但 SGD 在 full epoch 下对顺序不敏感(只影响方差,不影响期望) |

### 4.2 潜在风险场景

| 场景 | 风险 | 缓解 |
|---|---|---|
| Early stop / 有限 step 训练 | 本地数据先消费,未消费部分偏向远端 | 保证 step 数覆盖全池,或关闭 `locality_aware` |
| Curriculum learning | 数据有预定义顺序,locality reorder 破坏顺序 | 关闭 `locality_aware` |
| Producer 调度与内容相关(如按长度分桶后分发到不同 producer) | placement 与内容相关,本地优先 = 内容 bias | 关闭 producer-local routing,或确保 producer 调度与内容解耦 |
| GRPO group 内部分样本远端迟到 | group 级重排优先本地 group,可能延后远端 group | 稳定排序保持 group 间顺序,work stealing 兜底长尾 |

### 4.3 Batch 方差影响

locality 优先使 batch 内样本节点聚集,batch 间方差略增。对 BN/LN 统计量有微小影响,RL 训练通常用大 batch + group normalization,影响在容忍范围内。

## 5. 跨节点率预期(理论估算)

### 5.1 Hash Routing + locality_aware=True

| ready 池深度 | 本地池 (N=8) | batch_size=32 | GET 跨节点率 |
|---|---|---|---|
| 256 (8x) | 32 | 32 | 0% |
| 128 (4x) | 16 | 32 | 50% |
| 64 (2x) | 8 | 32 | 75% |

PUT 跨节点率:100%。**理论估算(待实测)**:GET 50–60%,PUT 100%。

### 5.2 叠加 Producer-Local Routing

| 场景 | PUT 跨节点率 | GET 跨节点率 |
|---|---|---|
| co-located(producer 与 consumer 同节点) | 0% | 10–20% |
| distributed producer(producer 跨节点,每节点有 SU) | 0% | 20–30% |
| centralized producer(producer 集中,不启用 producer-local) | 100% | 50–60% |

co-located 场景 GET 仍有 10–20% 残留:producer 产出顺序与 consumer 消费顺序未必对齐,本地 ready 池可能不足 batch_size。

### 5.3 GRPO 场景的特殊性

GRPO 在 hash routing 下 locality 收益有限:group 内连续 idx 散落在不同 SU,每个 group 通常只有 1/N 本地样本。**GRPO 真正的 locality 收益要配合 producer-local routing**,让整 group 落在 producer 本地 SU。

## 6. 优劣势

### 优势

1. 无硬约束,任意部署配置可跑
2. 不切分 ready 池,保留 work stealing,长尾场景行为与 hash routing 一致
3. 向后兼容,默认关闭时行为不变
4. GET 与 PUT 两阶段正交,可独立启用

### 劣势

1. GET 跨节点率压不到 0%,典型场景仍有 10–20% 残留
2. 依赖 ready 池深度,池浅时跨节点率回升
3. producer-local 仅适用于 distributed producer
4. producer 产出不均时数据在 SU 间倾斜
5. producer-local 下 controller 需维护 `_idx_placement`,百万样本约 50MB

## 7. 替代方案对比

### 7.1 候选方案

| 方案 | 核心思路 | GET 跨节点率 | PUT 跨节点率 |
|---|---|---|---|
| **A. 本方案(hash + locality)** | hash routing + sampler 本地优先 reorder | 50–60% | 100% |
| **B. 本方案(producer-local + locality)** | A + PUT 路由到 producer 本地 SU | 10–20% (co-located) / 50–60% (centralized) | 0% (co-located) / 100% (centralized) |
| **C. Stride-Partition** | 按 `dp_size` 切分 ready 池,每个 rank 只消费本地分区 | 0% | 不优化 |
| **D. Replication** | PUT 时数据复制到所有节点的 SU | 0% | 200%(N 节点) |
| **E. Topology-Aware Shard Sampling** | 采样阶段按拓扑分片,每片绑定 SU | 0% | 不优化 |
| **F. RDMA Direct Transfer** | 绕过 storage 层,producer→consumer 直传 | N/A | N/A |

### 7.2 维度对比

| 维度 | A | B | C | D | E |
|---|---|---|---|---|---|
| 硬约束 | 无 | distributed producer | `dp_size==num_su` + SU-DP 共置 | 无 | shard 数 == SU 数 |
| 同步等待 | 无 | 无 | 有(长尾指数化) | 无 | 有(分片刚性) |
| 长尾容错 | 保留 work stealing | 保留 work stealing | 切池,丧失统计弹性 | 保留 | 切分,丧失弹性 |
| 额外存储开销 | 无 | 无 | 无 | N 倍 | 无 |
| GRPO 兼容 | 需组级重排 | 需组级重排 | 原生支持 | 原生支持 | 需组级分片 |
| 默认行为 | 关闭,opt-in | 关闭,opt-in | — | — | — |
| 实现复杂度 | 中 | 中高 | 低 | 低 | 中 |

### 7.3 选型建议

- **co-located / distributed producer**(对应 single_controller_demo 模式)→ **B**:GET+PUT 双端优化
- **centralized producer**(对应 multi-controller/relax demo 模式)→ **A**:仅优化 GET,producer 集中导致本地无 SU 可路由
- **长尾率极低(<5%)且能配齐 `dp_size==num_su`** → **C**:GET 0%,实现简单
- **数据量小且带宽充足** → **D**:无路由逻辑
- **不确定** → **A**:可后续叠加 B

### 7.4 为什么不选 C/D/E

- **C (Stride-Partition)**:切池丧失 work stealing,长尾场景(group 迟到、rollout 抖动)会导致某个 rank 长时间空等。RL 训练长尾普遍。
- **D (Replication)**:存储开销 N 倍,大模型 KV cache 场景不可接受;且 PUT 跨节点率反而升高(N 倍复制)。
- **E (Topology-Aware Shard)**:与 C 类似的刚性问题,且需要 shard 数严格匹配 SU 数,部署灵活性差。

本方案(A+B)牺牲 0% 跨节点的理论上限,换来无硬约束、无同步等待、无额外存储、向后兼容。

## 8. 失败场景与容错

| 场景 | 行为 | 结果 |
|---|---|---|
| sampler 未开启 `locality_aware` | 忽略 placement_map,不触发 topology report | 行为与改造前一致,零开销 |
| controller 无 `_su_node_map` | placement_map 为空 | sampler 退化为原行为 |
| placement_map 未命中某 idx | `get(idx)` 返回 None | idx 归入 remote 桶,仍可消费 |
| 本地 ready 池为空 | 全部从 remote 取 | GET 跨节点率 100%,与 hash routing 一致 |
| producer-local: 本节点无 SU | fallback 到 hash | 数据均匀散布,无卡死 |
| producer-local: 多本地 SU | `idx % len(local_sus)` 均衡 | 无单 SU 容量超限 |
| topology report ACK 超时 | 只记日志,不重试 | 存储正常,controller 退化为无 placement 模式 |
| NPU 设备合并嵌套张量 | 跳过 `as_nested_tensor`,回退 `NonTensorStack` | NPU 兼容,无功能损失 |

## 9. 使用方式

### 9.1 仅 GET 优化(hash routing,默认)

```python
sampler = GRPOGroupNSampler(n_samples_per_prompt=4, locality_aware=True)
# controller 用默认 hash routing,handshake 后自动获取 _su_node_map
# client 自动注入 consumer_node_ip
```

### 9.2 GET + PUT 双端优化(co-located / distributed producer)

```python
sampler = GRPOGroupNSampler(n_samples_per_prompt=4, locality_aware=True)
config = {"routing_policy": "producer_local"}
# manager = AsyncSimpleStorageManager(..., config=config)
```

### 9.3 关闭(默认行为)

```python
sampler = SequentialSampler()  # locality_aware=False,零额外开销
```

## 10. 待验证

- [ ] Metric 埋点:`get_data` / `put_data` 统计跨节点字节占比,长期验证收益
- [ ] 双机性能对比:co-located / distributed / centralized 三种部署模式下的实测跨节点率与端到端吞吐
- [ ] Bias 实测:有限 step 训练下,本地与远端样本的消费比例分布

## 11. 总结

本方案把 locality 能力下沉为所有 sampler 的通用能力,通过显式开关(默认关闭)让用户 opt-in。routing 侧(hash 或 producer-local)与 sampler 侧联合优化,placement 信息在 producer-local 下由 PUT 上报、在 hash 下由 controller 自算,两种 routing 都能正确工作。

- **阶段 1(GET 端)**:hash routing + sampler locality。理论 GET 跨节点率从 87.5% 降到 50–60%,PUT 维持 100%。适用于所有部署。
- **阶段 2(PUT 端,可选叠加)**:producer-local routing。co-located 场景理论 PUT 0%,配合阶段 1 GET 降到 10–20%。centralized producer 不适用。
- **Bias**:全数据消费场景下无 bias;有限 step / curriculum learning / producer 调度与内容相关时需关闭。

两阶段正交,可独立启用。

## 12. 待讨论

本方案已实现并验证功能正确性,但是否为较优方案、是否值得投入维护,仍有疑问:

1. **理论收益是否值得复杂度**:GET 跨节点率理论上从 87.5% 降到 50–60%(hash)或 10–20%(producer-local),但仍非 0%,且这些数字未经双机实测验证。引入的 sampler 改造、placement 上报、topology report 等机制是否划算?

2. **producer-local 的适用面窄**:仅 distributed producer 场景可用,centralized producer 无收益。维护一套只对部分场景生效的 routing 策略是否合理?

欢迎指出当前方案忽略的问题,或提出更优思路。
