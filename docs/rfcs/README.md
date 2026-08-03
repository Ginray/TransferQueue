# TransferQueue Locality-Aware Routing 方案集

本目录收录针对 SimpleStorage 跨节点 GET 优化的方案设计与 RFC。

## 问题背景

TransferQueue SimpleStorage 后端默认使用 hash routing(`global_idx % num_su`),在 N 节点部署下,每个 batch 的 GET 跨节点率约为 `(N-1)/N`(N=8 时 ≈ 87.5%)。跨节点 GET 在 10Gbps 网络下可占训练 step 时间的 10–20%,成为分布式 RL 训练的显著开销。

## 方案概览

| 方案 | 文档 | 跨节点率 | 硬约束 | 同步等待风险 | 状态 |
|---|---|---|---|---|---|
| **Locality-Aware Soft Scheduling (v4)** | [locality_aware_routing_v4.md](./locality_aware_routing_v4.md) | 由本地 ready pool 深度决定 | 无固定拓扑约束 | 无，本地不足立即偷取 | **Proposed MVP** |
| RL Workload-Aware Placement and Scheduling | [rl_workload_aware_scheduling_rfc.md](./rl_workload_aware_scheduling_rfc.md) | 按场景优化总生命周期流量 | 无固定拓扑约束 | 同步全局计划 / 异步有界偷取 | Future architecture |
| Placement-Aware + Sampler Locality(双阶段) | [locality_aware_routing_v3.md](./locality_aware_routing_v3.md) | 50–60% → 10–20%* | 无 | 无 | Prototype / Superseded by v4 |
| Stride Partition + Group Routing | [locality_aware_routing_v2.md](./locality_aware_routing_v2.md) | 0% | `dp_size==num_su` + 共置 | 有(长尾指数化) | Draft |
| Locality-Aware SimpleStorage Routing (v1) | [locality_aware_simplestorage_routing.md](./locality_aware_simplestorage_routing.md) | — | — | — | Superseded |

*50–60% 和 10–20% 是 v3 的未实测估算，不作为 v4 的收益承诺。

## 当前推荐 MVP:Locality-Aware Soft Scheduling (v4)

v4 保持确定性 sample/group routing，优先选择本地 work，本地不足时立即从全局 ready pool 补齐。它不要求上层 RL 框架修改调用方式。具体设计见 [locality_aware_routing_v4.md](./locality_aware_routing_v4.md)。

## 历史实现记录:Placement-Aware + Sampler Locality (v3)

以下内容记录 v3 原型及其实现进度。v3 的 producer-local 主路径已被 v4 取代，不再作为推荐落地方向。

### 核心思路(两阶段,正交可叠加)

**阶段 1 — GET 端优化(hash routing,默认)**:
1. **Sampler 通用能力下沉**:Locality 作为 `BaseSampler` 的通用能力,通过 `locality_aware=True` 显式 opt-in(默认 False)。每个 sampler 在自身语义约束内做本地优先:
   - `SequentialSampler`:local 子集优先,各自保持原序
   - `GRPOGroupNSampler`:group 级重排,本地样本多的 group 优先
   - `RankAwareSampler`:首个 rank 采样时本地优先 reorder
2. **Controller 双源 placement**:hash routing 下用 `_su_node_map` + `idx % num_su` 自算 placement_map(零额外开销);producer-local 下用 `_idx_placement`(storage_manager 在 `notify_data_update` 时上报的实际 placement)。
3. **Client 自动注入**:`async_get_meta` 自动注入 `consumer_node_ip`,用户代码无需改动。

**阶段 2 — PUT 端优化(producer-local routing,可选叠加)**:
4. **Producer-Local Routing**:storage_manager 配置 `routing_policy="producer_local"`,PUT 时路由到 producer 本节点 SU(而非 `idx % num_su` 散布)。本节点无 SU 自动 fallback hash routing。verl 分布式 producer 场景 PUT 0% 跨节点,配合阶段 1 sampler GET 降到 10–20%。vime 集中式 producer 不启用。
5. **Placement 上报**:producer-local 下,storage_manager 在 `notify_data_update` 时把实际 `idx_to_node_ip` 上报给 controller,controller 缓存到 `_idx_placement[partition_id]`,供 `get_metadata` 构建 placement_map 时优先使用。

### 跨节点率预期

| 部署 | 阶段 1 | 阶段 1+2 叠加 |
|---|---|---|
| GET 跨节点率 | 50–60% | 10–20%(verl)/ 50–60%(vime) |
| PUT 跨节点率 | 100% | 0%(verl)/ 100%(vime) |

### 为什么推荐

- **无硬约束**:不需要 `dp_size == num_su`,不需要 SU-DP 共置,任何部署配置都能跑
- **无同步等待副作用**:不切分 ready 池,保留 work stealing 能力,长尾场景不卡死(对比 v2 的 stride_partition)
- **无调参**:`locality_ratio` 已移除,本地有多少取多少,不足自动 fallback
- **默认关闭,显式 opt-in**:`locality_aware=False` 为默认,行为与改造前完全一致;用户确认有 locality 收益时再开启
- **GET/PUT 双端可优化**:阶段 1 优化 GET,阶段 2 叠加优化 PUT,verl 场景双端都接近最优
- **hash routing 零额外开销**:placement 由 controller 自算,`notify_data_update` 不带额外字段
- **向后兼容**:不启用时行为与现状完全一致

### 局限

- GET 跨节点率压不到 0%(本地池上限 1/N)
- 阶段 2 仅 verl 等分布式 producer 场景可用;vime 集中式 producer 不适用
- 需要修改 `GRPOGroupNSampler` 做组级重排才能兼容(组完整性保留)
- GRPO 在 hash routing 下 locality 收益有限(连续 idx 散落),真正收益要配合 producer-local routing

## 实现进度

### 阶段 1:GET 端优化(hash routing + sampler locality)

- [x] `BaseSampler` 加 `locality_aware` 参数 + `_partition_by_locality` 静态方法
- [x] `SequentialSampler` / `GRPOGroupNSampler` / `RankAwareSampler` 支持 locality
- [x] `controller.get_metadata` 构建 placement_map(hash 规则自算)
- [x] `client.async_get_meta` 自动注入 `consumer_node_ip`
- [x] storage_manager handshake 上报 `su_node_map`,controller 缓存
- [x] 单元测试:`tests/test_samplers.py` 47 个测试全过
- [x] 功能验证:hash routing 下 SequentialSampler local count 4/8(从 2/8 提升)

### 阶段 2:PUT 端优化(producer-local routing)

- [x] `AsyncSimpleStorageManager` 新增 `routing_policy` 参数(默认 `"hash"`,可选 `"producer_local"`)
- [x] `_group_by_producer_local()` + fallback 逻辑
- [x] `put_data` 接受 `producer_node_ip`,记录 `_idx_to_su[idx] = su_id`
- [x] `client.async_put` 自动注入 `producer_node_ip`
- [x] `notify_data_update` 加 `idx_to_node_ip` 参数,producer-local 下上报实际 placement
- [x] `controller` 接收 `idx_to_node_ip` 并缓存到 `_idx_placement[partition_id]`
- [x] `controller.get_metadata` 双源 fallback(explicit 优先,hash 补齐)
- [x] 功能验证:producer-local 下 GRPO group 级 locality 生效,consumer 取到全本地 group

### 阶段 3:Metric 埋点(未实现)

- [ ] `get_data` / `put_data` 跨节点字节占比统计
- [ ] 暴露为 metric,长期验证收益

## 使用方式

### 仅 GET 优化(hash routing,默认)

```python
sampler = SequentialSampler(locality_aware=True)
# 或
sampler = GRPOGroupNSampler(n_samples_per_prompt=4, locality_aware=True)
# controller 用默认 hash routing,client 自动注入 consumer_node_ip
```

### GET + PUT 双端优化(verl 场景)

```python
sampler = GRPOGroupNSampler(n_samples_per_prompt=4, locality_aware=True)
# storage_manager 配置 producer-local routing
config = {"routing_policy": "producer_local"}
# manager = AsyncSimpleStorageManager(..., config=config)
```

### 关闭 locality(默认行为)

```python
sampler = SequentialSampler()  # locality_aware=False,行为与改造前完全一致
```
