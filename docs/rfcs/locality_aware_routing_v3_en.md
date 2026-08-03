# RFC: Locality-Aware Routing for SimpleStorage


## 1. Background

SimpleStorage uses hash routing by default (`global_idx % num_su`). In an N-node deployment, the cross-node rate of a single GET batch is approximately `(N-1)/N`:

| N | Cross-node rate |
|---|---|
| 4 | 75% |
| 8 | 87.5% |
| 16 | 93.75% |

The built-in samplers (`SequentialSampler`, `GRPOGroupNSampler`, `RankAwareSampler`) are locality-unaware: the controller only passes `ready_indexes` (a plain idx list) to the sampler, which has no idea which SU each idx lives on and cannot make local-first decisions.

**Root cause**: routing decides data placement at PUT time, but that information is lost by GET time.

## 2. Overview

**The routing side records placement; the sampler side does local-first selection adaptively.** Both sides share the same placement info and are jointly optimized. The GET and PUT stages are orthogonal and can be enabled independently.

```
PUT (producer-local routing, optional)
  → route data to the producer's local SU, record _idx_to_su[idx] = su_id
  → notify_data_update carries idx_to_node_ip
  → controller caches into _idx_placement[partition_id][idx]

TOPOLOGY REPORT (on demand)
  → only when locality_aware=True, storage_manager reports su_node_map
  → controller caches into _su_node_map

GET
  → controller.get_metadata builds placement_map (use reported values first,
    fall back to hash rule for missing idx):
      1. _idx_placement[partition_id]  ← producer-local actual location (if reported)
      2. idx % num_su → _su_node_map[su_id]  ← hash rule self-computed
  → sampler.sample(consumer_node_ip, placement_map) does local-first within
    its own semantic constraints
```

## 3. Design Decisions

### 3.1 Locality as a General Capability, Not a Standalone Sampler

Locality is pushed down to `BaseSampler` as a general capability. Each sampler does local-first within its **own semantic constraints**:

| Sampler | Semantic constraint | Locality strategy |
|---|---|---|
| `SequentialSampler` | order | local subset + remote subset, each keeps original order, local first |
| `GRPOGroupNSampler` | group integrity | group-level reorder, groups with more local samples first |
| `RankAwareSampler` | rank consistency | local-first reorder on the first rank's sampling |

A standalone `LocalityBiasedSampler` is infeasible: it would override the sampler's semantic contract (order / group integrity / rank consistency), leaving users unable to satisfy both constraints. Locality should be an orthogonal dimension.

### 3.2 Explicit Switch, Default Off

Each sampler accepts `locality_aware: bool = False` in its constructor. Default-off rationale: locality optimization changes sample order, which has side effects on training that depends on strict order (e.g. curriculum learning).

Trigger conditions: `locality_aware` opt-in + `consumer_node_ip` present + `placement_map` non-empty. If any is missing, original logic runs.

### 3.3 Placement Sources

| Priority | Source | Applicable scenario | Overhead |
|---|---|---|---|
| 1 | `_idx_placement` (reported at PUT) | producer-local routing | each PUT carries extra `{idx: node_ip}` |
| 2 | `idx % num_su → _su_node_map` (controller self-compute) | hash routing | zero |

Under hash routing, placement is deterministic and the controller can self-compute; under producer-local routing, placement depends on the producer's location and must be reported explicitly.

### 3.4 Topology Report on Demand

`_su_node_map` is reported via a dedicated `TOPOLOGY_REPORT` message rather than crammed into the handshake body. The handshake ACK carries a `locality_aware` flag, and the report is triggered only when the flag is true. It reuses the handshake's synchronous ZMQ context to avoid the async-context-returns-coroutine issue. Best-effort: failure does not affect storage.

### 3.5 Producer-Local Multi-Local-SU Balancing

When multiple SUs are deployed on a single node, naive producer-local routing sends all samples to the first local SU, causing capacity overflow. `_group_by_producer_local` uses a three-way strategy:

| Scenario | Routing strategy |
|---|---|
| No `producer_node_ip`, or no SU on this node | fall back to global hash routing |
| Exactly 1 local SU | route all to it |
| Multiple local SUs | `global_idx % len(local_sus)` hash-balanced across local SUs |

### 3.6 GRPO Group-Level Reorder

GRPO's hard constraint is group integrity, so the reorder unit is a group. The implementation changes from "scan-and-pick" to "collect all complete groups first, stable-sort by local sample count descending, then pick the top N". Stable sort preserves the original scan order when locality is tied (backward compatible).

## 4. Bias Analysis

**Conclusion: no bias is introduced under the full-data-consumption training paradigm.**

### 4.1 Why No Bias

| Dimension | Analysis |
|---|---|
| Placement-content correlation | hash routing: `idx % num_su` is content-independent; producer-local: if producer scheduling is content-independent (e.g. random shuffle then uniform dispatch), placement is also content-independent |
| Sampling coverage | sampler does not split the ready pool, work stealing is preserved; local shortfall is filled from remote; the entire pool is eventually consumed |
| Sampling order | locality changes intra-batch order, but SGD is insensitive to order under a full epoch (affects variance, not expectation) |

### 4.2 Risk Scenarios

| Scenario | Risk | Mitigation |
|---|---|---|
| Early stop / limited-step training | local data consumed first, unconsumed part skews remote | ensure steps cover the full pool, or disable `locality_aware` |
| Curriculum learning | data has a predefined order, locality reorder breaks it | disable `locality_aware` |
| Producer scheduling is content-correlated (e.g. length-bucketed dispatch to different producers) | placement correlates with content, local-first = content bias | disable producer-local routing, or decouple producer scheduling from content |
| GRPO group with some remote samples arriving late | group-level reorder prefers local groups, may delay remote groups | stable sort preserves inter-group order, work stealing handles tail |

### 4.3 Batch Variance

Local-first makes intra-batch samples node-clustered, slightly increasing inter-batch variance. The impact on BN/LN statistics is minor; RL training typically uses large batches + group normalization, within tolerance.

## 5. Expected Cross-Node Rates

### 5.1 Hash Routing + locality_aware=True

| Ready pool depth | Local pool (N=8) | batch_size=32 | GET cross-node rate |
|---|---|---|---|
| 256 (8x) | 32 | 32 | 0% |
| 128 (4x) | 16 | 32 | 50% |
| 64 (2x) | 8 | 32 | 75% |

PUT cross-node rate: 100%. **Typical expectation**: GET 50–60%, PUT 100%.

### 5.2 With Producer-Local Routing

| Scenario | PUT cross-node rate | GET cross-node rate |
|---|---|---|
| co-located (producer and consumer on the same node) | 0% | 10–20% |
| distributed producer (producer cross-node, each node has an SU) | 0% | 20–30% |
| centralized producer (producer centralized, producer-local disabled) | 100% | 50–60% |

The co-located case still has 10–20% GET residual: producer output order and consumer consumption order may not align, leaving the local ready pool short of batch_size.

### 5.3 GRPO Specifics

GRPO has limited locality benefit under hash routing: consecutive idx within a group are scattered across SUs, and each group typically has only 1/N local samples. **GRPO's real locality benefit requires producer-local routing**, so the entire group lands on the producer's local SU.

## 6. Pros and Cons

### Pros

1. No hard constraints, works with any deployment config
2. Does not split the ready pool, preserves work stealing, tail behavior matches hash routing
3. Backward compatible, default-off behavior unchanged
4. GET and PUT stages are orthogonal, can be enabled independently

### Cons

1. GET cross-node rate cannot reach 0%, typical residual 10–20%
2. Depends on ready pool depth, shallow pool raises cross-node rate
3. producer-local only applies to distributed producer
4. Uneven producer output skews data across SUs
5. Under producer-local, controller must maintain `_idx_placement`, ~50MB per million samples
6. GRPO requires group-level reorder, complex to implement

## 7. Comparison with Alternatives

### 7.1 Candidate Options

| Option | Core idea | GET cross-node | PUT cross-node |
|---|---|---|---|
| **A. This proposal (hash + locality)** | hash routing + sampler local-first reorder | 50–60% | 100% |
| **B. This proposal (producer-local + locality)** | A + PUT routes to producer's local SU | 10–20% (co-located) / 50–60% (centralized) | 0% (co-located) / 100% (centralized) |
| **C. Stride-Partition** | split ready pool by `dp_size`, each rank consumes only its local partition | 0% | not optimized |
| **D. Replication** | PUT replicates data to all nodes' SUs | 0% | 200% (N nodes) |
| **E. Topology-Aware Shard Sampling** | topology-based sharding at sampling time, each shard bound to an SU | 0% | not optimized |
| **F. RDMA Direct Transfer** | bypass storage layer, producer→consumer direct transfer | N/A | N/A |

### 7.2 Dimension Comparison

| Dimension | A | B | C | D | E |
|---|---|---|---|---|---|
| Hard constraints | none | distributed producer | `dp_size==num_su` + SU-DP co-location | none | shard count == SU count |
| Sync wait | none | none | yes (tail amplifies) | none | yes (rigid sharding) |
| Tail tolerance | preserves work stealing | preserves work stealing | pool split, loses statistical elasticity | preserved | split, loses elasticity |
| Extra storage | none | none | none | N× | none |
| GRPO compatibility | needs group-level reorder | needs group-level reorder | native | native | needs group-level sharding |
| Default behavior | off, opt-in | off, opt-in | — | — | — |
| Implementation complexity | medium | medium-high | low | low | medium |

### 7.3 Selection Guide

- **co-located / distributed producer** (corresponds to single_controller_demo) → **B**: GET+PUT dual-end optimization
- **centralized producer** (corresponds to multi-controller / relax demo) → **A**: only GET optimized, producer is centralized so no local SU to route to
- **Very low tail rate (<5%) and can match `dp_size==num_su`** → **C**: GET 0%, simple implementation
- **Small data volume, sufficient bandwidth** → **D**: no routing logic
- **Uncertain** → **A**: can stack B later

### 7.4 Why Not C/D/E

- **C (Stride-Partition)**: splitting the pool loses work stealing; tail cases (late group, rollout jitter) leave a rank waiting idle for a long time. Tails are common in RL training.
- **D (Replication)**: N× storage overhead is unacceptable for large-model KV cache; PUT cross-node rate also rises (N× replication).
- **E (Topology-Aware Shard)**: same rigidity issue as C, and shard count must strictly match SU count, poor deployment flexibility.

This proposal (A+B) trades the theoretical 0% ceiling for no hard constraints, no sync wait, no extra storage, and backward compatibility.

## 8. Failure Modes and Tolerance

| Scenario | Behavior | Result |
|---|---|---|
| sampler has `locality_aware` off | ignores placement_map, no topology report | behavior unchanged, zero overhead |
| controller has no `_su_node_map` | placement_map empty | sampler degrades to original behavior |
| placement_map misses an idx | `get(idx)` returns None | idx goes to remote bucket, still consumable |
| local ready pool empty | all from remote | GET cross-node rate 100%, same as hash routing |
| producer-local: no SU on this node | fall back to hash | data evenly distributed, no stall |
| producer-local: multiple local SUs | `idx % len(local_sus)` balanced | no single-SU capacity overflow |
| topology report ACK timeout | log only, no retry | storage works, controller degrades to no-placement mode |
| NPU device merging nested tensors | skip `as_nested_tensor`, fall back to `NonTensorStack` | NPU compatible, no functional loss |

## 9. Usage

### 9.1 GET-only optimization (hash routing, default)

```python
sampler = GRPOGroupNSampler(n_samples_per_prompt=4, locality_aware=True)
# controller uses default hash routing, obtains _su_node_map after handshake
# client auto-injects consumer_node_ip
```

### 9.2 GET + PUT dual-end optimization (co-located / distributed producer)

```python
sampler = GRPOGroupNSampler(n_samples_per_prompt=4, locality_aware=True)
config = {"routing_policy": "producer_local"}
# manager = AsyncSimpleStorageManager(..., config=config)
```

### 9.3 Disabled (default behavior)

```python
sampler = SequentialSampler()  # locality_aware=False, zero overhead
```

## 10. To Verify

- [ ] Metric instrumentation: `get_data` / `put_data` cross-node byte ratio, long-term benefit validation
- [ ] Dual-node performance comparison: measured cross-node rate and end-to-end throughput under co-located / distributed / centralized deployments
- [ ] Bias measurement: local-vs-remote consumption ratio distribution under limited-step training

## 11. Summary

This proposal pushes locality down as a general capability of all samplers, with an explicit switch (default off) for opt-in. The routing side (hash or producer-local) and the sampler side are jointly optimized; placement info is reported by PUT under producer-local and self-computed by the controller under hash, so both routing modes work correctly.

- **Stage 1 (GET)**: hash routing + sampler locality. GET cross-node rate drops from 87.5% to 50–60%, PUT stays 100%. Applies to all deployments.
- **Stage 2 (PUT, optional, stackable)**: producer-local routing. co-located PUT 0%, combined with Stage 1 GET drops to 10–20%. Not applicable to centralized producer.
- **Bias**: no bias under full-data consumption; must be disabled for limited-step / curriculum learning / content-correlated producer scheduling.

The two stages are orthogonal and can be enabled independently.

## 12. Open Questions

This proposal is implemented and functionally verified, but whether it is the better option and whether it is worth maintaining remains uncertain:

1. **Is the theoretical benefit worth the complexity?** GET cross-node rate theoretically drops from 87.5% to 50–60% (hash) or 10–20% (producer-local), but still not 0%, and these numbers have not been verified by dual-node measurement. Are the sampler changes, placement reporting, and topology report mechanisms worth it?

2. **producer-local has a narrow applicability.** Only the distributed-producer scenario benefits; centralized producer gains nothing. Is it reasonable to maintain a routing strategy that only works for some scenarios?

Feedback on issues overlooked by this proposal, or better ideas, is welcome.
