# A2-26 <-> A2-27 HIXL rH2H experiment and TQ selection note

> This file is a historical validation record for one test cluster. Paths,
> addresses, devices, versions, and commands are not TransferQueue product configuration.

Date: 2026-08-04  
Scope: decide whether HIXL can be the RDMA backend for TransferQueue
`SimpleStorage` Host payloads on the current A2-26/27/28 cluster.

## Executive conclusion

HIXL `rH2H` is documented as supported for **A2 RoCE**, but it is not a
CPU-only Host-NIC RDMA abstraction on the exercised implementations. Both
available HIXL connection paths require Ascend device communication resources:

| HIXL path | How selected | Actual dependency | A2-26 <-> 27 result |
| --- | --- | --- | --- |
| legacy/default | no `LocalCommRes`, which is the benchmark default | ADXL -> HCCL communicator -> HCCL one-sided Get/Put | fails because generated rank table has an empty NPU `device_ip` |
| 1.3 / recommended | `LocalCommRes={"version":"1.3"}` | HixlCS endpoint generation; reads NPU HCCN IP from `hccn.conf` or `hccn_tool` | fails before connection because NPU 0 has no HCCN IP |

Therefore HIXL is **not yet a Go** for TQ SimpleStorage on this cluster. This
is an environment-precondition result, not a claim that A2 `rH2H` is absent.
No successful transfer or bandwidth number has been measured.

For a TQ backend that must work on CPU-only nodes or must use only the Host
RoCE NIC, HIXL has the wrong dependency boundary. It can be reconsidered for
an Ascend deployment after HCCN is configured and an end-to-end HIXL test
passes. A Host RDMA backend should otherwise use an independent Host transport
(UCX or a small native-Verbs service).

## What the official 9.1.0 benchmark states

Source inspected: HIXL branch `9.1.0`, commit
`c12f9a56ab66299f62adb7f1f3f34d92e725e856`.

`benchmarks/README.md` defines:

| direction | meaning | operation |
| --- | --- | --- |
| `H2rH` | Host writes remote Host | write |
| `rH2H` | Host reads remote Host | read |

The same document's support table says the A2 RoCE path supports all eight
memory directions, including these two. Its documented dual-host invocation
is:

```bash
# target host
python3 benchmarks/comm_benchmark/scripts/run_comm_benchmark.py \
  --role=target --transport=rdma

# initiator host: run the command printed by target
```

This proves the intended product capability. It does not by itself establish
that a bare Host NIC can be used without an Ascend communication environment.

## Environment actually used

| item | A2-26 | A2-27 |
| --- | --- | --- |
| host RoCE test address | `178.123.4.4` | `178.123.4.3` |
| NPU | Ascend 910B3 (A2) | Ascend 910B3 (A2) |
| driver / HDK | 25.5.1 | 25.5.1 |
| installed Toolkit and 910B ops | 9.1.0-beta.1 | 9.1.0-beta.1 |
| default `/usr/local/Ascend/cann` | **9.0.0** | 9.1.0-beta.1 |

All benchmark commands explicitly sourced:

```bash
source /usr/local/Ascend/cann-9.1.0-beta.1/set_env.sh
```

On A2-27, the official source branch was built exactly as documented:

```bash
cd /tmp/hixl-v910-benchmark
bash build.sh --examples
```

The build completed and produced `hixl_comm_bench`. The generated HIXL package
was **not installed**; system CANN, driver, HCCN, and network configuration
were not changed. A2-26 lacks `cmake`, so it used the same temporary benchmark
binary and temporary source-built `libcann_hixl.so` copied from A2-27.

## Experiment A: benchmark default path

Test intent:

```text
target:    A2-27, 178.123.4.3:16000
initiator: A2-26, 178.123.4.4:16001
direction: rH2H
transport: rdma
block:     64 KiB
```

Observed before failure:

1. Both processes initialized HIXL.
2. Both allocated and registered Host memory with `aclrtMallocHost`.
3. TCP coordination connected and exchanged the remote Host address.
4. The initiator failed during HIXL `Connect`.

Exact failure:

```text
Config_Error_Ranktable(EI0014): Value [] for ranktable variable [device_ip] is invalid
HcclCommInitClusterInfoMemConfig ... fail
```

### Why this default used HCCL

This is source behavior, not an inference from the error:

```text
EngineFactory::CreateEngine
  no LocalCommRes option
  -> CommEngine
  -> adxl::AdxlInnerEngine
  -> CommChannel::InitializeHcclComm
  -> HcclCommInitClusterInfoMemConfig

CommChannel::TransferSync(read)
  -> HcclBatchGet
```

Relevant source files:

- `src/hixl/engine/engine_factory.cc`
- `src/hixl/engine/comm_engine.cc`
- `src/llm_datadist/adxl/comm_channel.cc`
- `src/llm_datadist/hccl/hccl_adapter.cc`

The default benchmark supplies `BufferPool=0:0` but no `LocalCommRes`; it
therefore intentionally selects this compatibility/collective-communicator
backend.

## Experiment B: documented 1.3 path

The current `docs/cpp/HIXL接口.md` distinguishes the paths:

```text
LocalCommRes empty / 1.0 / 1.2 -> collective-communication-domain connection
LocalCommRes = {"version":"1.3"} -> HixlCS connection (recommended)
                                    requires HDK >= 25.5 and Toolkit >= 9.1
```

We reran the official target command with:

```bash
-H='LocalCommRes={"version":"1.3"}'
```

Important benchmark syntax: the executable requires the `-H=KEY=VALUE` form;
`-H KEY=VALUE` is rejected by its parser.

The target accepted the option and its effective configuration showed:

```text
LocalCommRes={"version":"1.3"}
```

It then failed **before** TCP/HIXL connection:

```text
Failed to get device ip from hccn.conf and hccn_tool, phy_device_id:0
  -> EndpointGenerator::BuildRoceEndpoint
  -> EndpointGenerator::BuildEndpointList
  -> HixlEngine::Initialize
```

This agrees with the generic HIXL source: `LocalCommRes` version 1.3 selects
`HixlEngine`; its `DirectClientHandler` calls HixlCS APIs. This run did not
reach a data transfer, so it does not prove the eventual on-wire data path.
It does prove that HixlCS needs an NPU HCCN IP in order to construct its RoCE
endpoint.

## Live HCCN check

On both hosts, NPU 0 and NPU 1 currently return:

```text
Get ipconf failed, because no ip was preset there!
```

Thus the failures are expected from the current device network state:

```text
default path: HCCL rank table needs device_ip
1.3 path:     HixlCS endpoint generator needs device_ip
```

The Host NIC addresses (`178.123.4.4` and `178.123.4.3`) do not substitute for
per-NPU HCCN IPs.

## Implications for TransferQueue

### HIXL option

Use HIXL only if all of the following are acceptable:

- each TQ worker has an Ascend device and an ACL runtime context;
- the participating NPU HCCN IPs and links are configured and reachable;
- a benchmark verifies the exact HIXL route selected by the application;
- TQ accepts that a Host payload transport has an NPU/HCCN lifecycle and
  operational dependency.

After HCCN is made available, rerun the 1.3 test first, then validate 64 KiB,
1 MiB, and 16 MiB in both directions, concurrency, reconnect, and peer exit.

### Independent Host RDMA option

Choose an independent Host transport if SimpleStorage must run without NPU or
HCCN setup. On this cluster, ordinary Host Verbs bandwidth testing has already
worked, whereas UCX `rc_verbs` is separately blocked by an HNS UD-QP bootstrap
issue. That is a UCX/provider compatibility problem, not a test of HIXL.

Do not claim a TQ speedup until the selected transport has passed the full
payload lifecycle and has measured end-to-end timings.

## Reference implementations and corrected comparison

This section separates verified implementation facts from an adoption
recommendation. A framework that can register an accelerator buffer does not
automatically provide every H2H/H2D/D2H/D2D direction as a portable backend.

| system / option | confirmed implementation boundary | useful TQ lesson | qualification |
| --- | --- | --- | --- |
| Mooncake | Its C++ RDMA transport directly manages Verbs resources: PD, CQ, QP, MR and `ibv_reg_mr`. Its CUDA build distinguishes Host and device pointers and can use dma-buf or NVIDIA peer-memory registration for device RDMA. Recent releases also contain Ascend HIXL RoCE samples and Ascend-direct work. | Reuse the design idea: a uniform transfer interface above native, accelerator-specific transports; cache registrations and make teardown explicit. | Do **not** summarize this as “Mooncake Ascend = HIXL with every memory direction proven”. Exact Ascend behavior is release-, CANN-, hardware-, and route-dependent. |
| YuanRong DataSystem TransferEngine | Python API is pybind11 (`yr.datasystem.TransferEngine`). It exposes initialization, registration, synchronous read and batch synchronous read. Its Python API requires `npu:${id}` and does not expose write or async operations. The current source tree has recent HIXL D2D backend work. | Keep the Python surface narrow and isolate the native backend behind one engine interface; preserve a fallback path. | It is **not** evidence of a generic Python Host H2H RDMA backend, nor evidence that all NPU H2H/H2D/D2H/D2D directions are exposed. |
| UCX/UCP | UCP provides C/C++ high-level communication APIs (Tag, AM, RMA) over multiple transports and supports CUDA memory when built with CUDA support. It may select GPUDirect RDMA when the build, NIC, driver, GPU topology and selected transport permit it. | Best initial portable candidate for a Host/GPU C++ backend when the target environment is known-good. | Tag or AM use does not itself guarantee zero-copy/GDR. On these A2 hosts, UCP `rc_verbs` is not currently usable because the HNS UD-QP bootstrap fails; do not declare UCX a current Go without fixing or bypassing that compatibility issue. |
| HIXL | On A2, the benchmark documents `rH2H`, but the tested default and 1.3 routes both require NPU communication configuration. | Use only as the Ascend-specific backend, with lifecycle tied to ACL and HCCN. | Not a CPU-only Host-NIC RDMA backend on the current cluster. |
| native Verbs | Direct `libibverbs` gives complete control of Host RC QPs, registrations, completions, and connection metadata exchange. Basic Host Verbs traffic has passed in this environment. | Keep as a targeted fallback when UCX cannot support a provider/topology. | Highest implementation and operational cost; do not add it merely as a speculative performance optimization. |

### TQ backend selection, revised

```text
TQ Python layer
  thin pybind11 interface only; it does not poll CQs or own RDMA lifecycle

Host H2H
  candidate 1: UCX/UCP after provider-specific acceptance passes
  candidate 2: a small C++ Verbs backend if UCX remains incompatible
  fallback: existing TCP/ZMQ path

NVIDIA GPU
  CUDA-aware UCX backend; enable/measure GDR only when runtime capability
  detection proves it is active

Ascend NPU
  HIXL backend; require compatible CANN/HIXL, ACL context, HCCN IP and links
```

The initial TQ proposal must **not** be described as a low-cost simultaneous
implementation of `UcpBackend + HixlBackend + TcpBackend`. These are three
independent lifecycle, registration and failure-recovery systems. A practical
sequence is: preserve TCP/ZMQ fallback, validate one selected Host backend,
then add the Ascend-specific HIXL backend only after its HCCN acceptance gate
passes.

## Decision status

| decision question | status |
| --- | --- |
| Does 9.1.0 benchmark document A2 `rH2H`? | Yes |
| Does the current 26/27 environment meet the HCCN prerequisite? | No |
| Did default HIXL `rH2H` transfer pass? | No, HCCL `device_ip` absent |
| Did HixlCS 1.3 transfer pass? | No, HixlCS endpoint `device_ip` absent |
| Is HIXL a CPU-only SimpleStorage RDMA backend on this cluster? | No |
| Is HIXL permanently ruled out for A2? | No; configure HCCN and rerun 1.3 |
