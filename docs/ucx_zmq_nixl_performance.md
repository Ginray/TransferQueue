# SimpleStorage ZMQ / UCX / NIXL-UCX 性能与 VIME 验证报告

> 更新于 2026-08-25：删除 arithmetic prompt、Qwen3-0.6B smoke 和重复四题长跑记录，保留性能矩阵、固定 payload 精度 replay，以及 80 道真实 DAPO 题的 20-step 对照。

## 当前结论

- SimpleStorage 的 ZMQ、UCX、NIXL-UCX Host payload 路径已完成跨节点 PUT/GET/CLEAR 和数据校验，完整矩阵 33/33 个 case 为 PASS。
- 固定同一批真实 rollout payload 后，ZMQ 与 NIXL-UCX 的 tokens、长度、mask、reward、raw reward、截断标记、sample index、rollout logprob digest 完全一致；loss、entropy loss、logp diff 完全一致，未发现 NIXL-UCX 引入的数据或精度损坏。
- 单机 VIME + TQ + NIXL-UCX 使用 80 道不重复的真实 DAPO 题完成 20/20 step；ZMQ 对照也完成 20/20 step。两条链路的 raw reward 和截断率处于同一范围，没有出现 NIXL 独有的 reward 崩溃、乱码、无法终止或 OOM。
- 20-step 长跑是两次独立 rollout 生成，因此不要求逐 step token/reward 完全相等；严格精度结论以固定 payload replay 为准，长跑用于观察功能、生命周期和 reward/truncation 趋势。
- 单机配置使用 UCX_TLS=sm,self,tcp、UCX_NET_DEVICES=lo，验证的是本地 NIXL-UCX 集成，不代表 RoCE/RDMA 或跨节点 VIME 已验收。

## 1. SimpleStorage 跨节点性能

测试于 2026-08-24 在 A2-26 (178.123.4.4) 和 A2-27 (178.123.4.3) 完成，Ray 两节点集群，128 KiB 至 1 GiB，共 33 个 case，数据校验全部通过。表中为 PUT/GET median 吞吐，单位 MiB/s。

| payload | ZMQ | UCX | NIXL-UCX |
| ---: | ---: | ---: | ---: |
| 128 KiB | 10.23 / 10.45 | 26.04 / 27.63 | 27.95 / 26.06 |
| 256 KiB | 20.67 / 20.46 | 35.75 / 35.02 | 43.03 / 41.49 |
| 512 KiB | 35.09 / 34.04 | 53.93 / 53.34 | 54.58 / 58.01 |
| 1 MiB | 51.73 / 51.88 | 72.23 / 72.35 | 79.22 / 77.70 |
| 4 MiB | 84.07 / 85.04 | 97.43 / 95.85 | 98.44 / 97.22 |
| 16 MiB | 104.47 / 104.33 | 106.75 / 106.65 | 106.16 / 106.04 |
| 64 MiB | 109.92 / 109.01 | 109.38 / 109.22 | 108.42 / 108.30 |
| 128 MiB | 111.05 / 109.85 | 109.83 / 109.77 | 108.79 / 108.71 |
| 256 MiB | 111.65 / 110.54 | 110.07 / 110.06 | 109.00 / 109.01 |
| 512 MiB | 111.76 / 111.58 | 110.22 / 110.19 | 109.11 / 109.11 |
| 1 GiB | 112.09 / 111.92 | 110.30 / 110.29 | 109.17 / 109.18 |

![ZMQ、UCX、NIXL-UCX 吞吐对比](assets/ucx_zmq_nixl_throughput.svg)

128 KiB～4 MiB 时 UCX/NIXL-UCX 明显快于 ZMQ；64 MiB 以上三种方式都接近链路上限，当前实现下 NIXL-UCX 大 payload 比原生 UCX 略慢约 1%。

结果文件：

~~~text
../tmp/results/simplestorage_payload_20260824/summary.csv
../tmp/results/simplestorage_payload_20260824/summary.md
~~~

## 2. 真实 DAPO 长响应验证

使用 /home/datasets/dapo-math-17k/dapo-math-17k.jsonl、Qwen3-4B-Instruct-2507，固定 seed=1234、rollout_seed=42、关闭 shuffle、开启 eager/deterministic inference、n_samples_per_prompt=4。

| 实验 | response len mean/min/max | truncated | repetition | raw reward | entropy loss | logp diff |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ZMQ, max=8192 | 8192 / 8192 / 8192 | 1.0 | 0.0 | 0.0 | 0.1633633 | 0.0072657 |
| NIXL-UCX, max=20000 | 14600.75 / 8361 / 20000 | 0.5 | 0.5 | 0.0 | 0.1346515 | 0.0061354 |

两次运行的 loss、pg_loss、ppo_kl 和 grad_norm 均为 0，原因是该组真实样本 reward 全为 0；不能把 loss=0 当成精度通过。NIXL-UCX 20K 运行没有 OOM、NaN、Traceback、NIXL_ERR 或 UCX error。20K 输出没有二进制乱码；已观察到的异常是模型重复输出和错误答案，不能归因于 NIXL-UCX。

## 3. 固定真实 payload 的 ZMQ/NIXL-UCX 精度 replay

先用 ZMQ 生成并保存一批固定的真实 DAPO rollout，再分别通过 ZMQ 和 NIXL-UCX replay 同一个 /tmp/vime_fixed_payload/0.pt。配置为 Qwen3-4B-Instruct-2507、8 个真实 prompt、每个 prompt 4 个 sample、global batch=32、seed=1234、rollout_seed=42、response max=1024。

| 指标 | ZMQ replay | NIXL-UCX replay | 结论 |
| --- | ---: | ---: | --- |
| raw reward | 0.375 | 0.375 | 一致 |
| response length | 801.5625 | 801.5625 | 一致 |
| truncated | 0.53125 | 0.53125 | 一致 |
| loss | -7.450580596923828e-09 | -7.450580596923828e-09 | 一致 |
| pg_loss | -7.450580596923828e-09 | -7.450580596923828e-09 | 一致 |
| entropy_loss | 0.17829982936382294 | 0.17829982936382294 | 一致 |
| logp diff | 0.0080028735101223 | 0.0080028735101223 | 一致 |
| ppo_kl | 0.0 | 0.0 | 一致 |
| grad_norm | 0.5211178066299796 | 0.5208552991685566 | 相对差约 0.05% |

两条链路的 TQ_PAYLOAD_DIGEST 在 put_pre 和 get_post 间，以及 ZMQ/NIXL 两条链路之间，以下 9 类字段全部相同：

~~~text
tokens             86dcf2b079c9cd20
total_lengths      20bc71384c5f9c29
response_lengths   bf640a8a8723e22d
loss_masks         2ca2a537e34555fa
rewards            3aa04748d69894a3
raw_reward         86963b33d6f04636
truncated          d0576707697269c0
sample_indices     4459459e1764134c
rollout_log_probs  802f4fe222ef83f8
~~~

这组 replay 是当前判断 NIXL 是否引入精度问题的主要证据：传输前后的训练输入和 rollout logprob 相同，前向 entropy、loss、logp diff 相同；grad norm 的微小差异属于独立训练进程的运行时数值差异。

证据文件：

~~~text
/tmp/vime_fixed_payload/0.pt
/tmp/vime_zmq_fixed_payload_replay_20260825.log
/tmp/vime_nixl_ucx_fixed_payload_replay_20260825.log
~~~

## 4. 80 道真实 DAPO 题的 20-step reward 对照

从真实 DAPO 原题库确定性选择 80 道不重复题目，题面长度约 260～784 字符；两条链路均使用同一个 /tmp/dapo_diverse_long_80.jsonl，每 step 4 个 prompt × 4 个 sample，global batch=16，response max=2048，vLLM max model len=4096，固定 seed=1234、rollout_seed=42、关闭 shuffle，并开启 eager/deterministic inference。

![80 道真实 DAPO 题的 20-step ZMQ/NIXL-UCX reward 与截断率](assets/vime_reward_20step_diverse_dapo_zmq_nixl.png)

| 指标 | ZMQ | NIXL-UCX |
| --- | ---: | ---: |
| 完成 step | 20/20 | 20/20 |
| raw reward mean/min/max | 0.25625 / 0.0 / 0.625 | 0.221875 / 0.0 / 0.5 |
| truncated mean/min/max | 0.734375 / 0.375 / 1.0 | 0.765625 / 0.5 / 1.0 |
| response length mean | 1810.531 | 1819.287 |
| entropy loss mean | 0.239102623 | 0.238708921 |
| logp diff mean | 0.009636145 | 0.009639767 |

两次长跑都出现了 raw reward=0 和 response 达到 2048 的 step，也都出现了非零 reward 和较低截断率的 step；未观察到 NIXL-UCX 特有的全程 reward=0、输出乱码、无法终止或 OOM。两次实验是独立生成，故不能用逐 step 的 reward 差异作为 bitwise 精度结论。

逐 step 原始数据：

~~~text
../tmp/results/vime_diverse_dapo_20step_metrics.csv
~~~

原始日志：

~~~text
/tmp/vime_zmq_diverse_real_20step_20260825.log
/tmp/vime_nixl_ucx_diverse_real_20step_20260825.log
~~~

## 5. 最终验收边界

当前可以确认：

> 单机 VIME + TQ + NIXL-UCX 的真实数据通路支持 20-step 长生命周期运行；固定真实 rollout replay 时，ZMQ 与 NIXL-UCX 的传输字段、entropy loss、logp diff 和 loss 一致，未发现 NIXL-UCX 引入的精度问题。

当前不能确认：

- RoCE/RDMA 或跨节点 VIME 拓扑已经验收；
- 两条 transport 在重新生成 rollout 时逐 token、逐 step bitwise 一致；
- 真实 mixed-reward 数据上的长期训练收敛一致。

后续若要继续扩展，应优先提高真实 DAPO 长跑的 response 上限或按题目长度分桶，降低当前 2048 配置的高截断率；精度对照继续使用固定 payload replay。
