# SimpleStorage NIXL-UCX Host Payload 传输

将 `payload_transfer` 设置为 `nixl-ucx` 后，所有非空 payload 都通过 NIXL-UCX
传输，ZMQ 只负责控制消息。本文介绍具体的配置和使用方法。

## 1. 确认 RDMA 设备

在每个运行 TQ 或 Ray worker 的节点上执行：

```bash
ls /sys/class/infiniband
rdma link show
ibv_devinfo
```

`ls` 应列出 RDMA 设备，`rdma link show` 中的端口应为 `ACTIVE`。`ibv_devinfo` 不会
显示 provider 动态库名称。如果没有设备或端口未激活，先检查驱动、`rdma-core`、
provider 和容器设备映射。

## 2. 安装 NIXL 和 TQ

安装 NIXL wheel：

```bash
python -m pip install nixl
```

NIXL wheel 包含 UCX 运行库，系统仍要安装 `rdma-core`、`libibverbs` 和网卡对应的
provider。

在 TQ 源码目录安装 TQ：

```bash
python -m pip install -e .
```

## 3. 检查 NIXL-UCX 后端

运行：

```bash
python - <<'PY'
from nixl import nixl_agent, nixl_agent_config

config = nixl_agent_config(
    enable_prog_thread=True,
    enable_listen_thread=True,
    listen_port=0,
    backends=["UCX"],
)
agent = nixl_agent("tq-nixl-check", config)
assert "UCX" in agent.backends
print("NIXL UCX backend is available")
PY
```

命令应输出 `NIXL UCX backend is available`。如果报错，查看文末的常见问题。

## 4. 启用 SimpleStorage NIXL-UCX 传输

在 TQ 配置中启用 NIXL-UCX：

```yaml
backend:
  storage_backend: SimpleStorage
  SimpleStorage:
    payload_transfer:
      backend: nixl-ucx
      ucx_env_vars: {}
```

`ucx_env_vars: {}` 表示 TQ 不额外设置 UCX 环境变量。TQ 和 Ray worker 会继续使用启动
它们时继承的 `UCX_*`。要指定 transport、设备或 GID，把相应变量写入
`ucx_env_vars`。

NIXL 初始化或传输失败时，TQ 会直接报错，不会退回 ZMQ。未限制 transport，或
`UCX_TLS` 包含 `tcp` 时，UCX 可能使用 TCP。

### 常用 UCX 配置

| 变量 | 用途 | 参考值 |
| --- | --- | --- |
| `UCX_TLS` | 限制 UCX 可用的 transport | `<可用的 rc_* transport>,tcp,sm,self` |
| `UCX_NET_DEVICES` | 指定 RDMA 设备和端口 | `<rdma_device>:<port>` |
| `UCX_IB_GID_INDEX` | 指定 RoCE GID 索引 | `<gid_index>` |
| `UCX_MODULE_DIR` | 指定 NIXL wheel 的 UCX transport 模块目录 | `<ucx_module_dir>` |

修改后重启 TQ/Ray。设备名和 GID 索引按节点填写。

### 内存注册

NIXL 注册内存前，先查看当前 shell 的系统限额：

```bash
ulimit -l
```

如果数值过小，在启动 TQ/Ray 的 shell 中调整为 `unlimited`：

```bash
ulimit -l unlimited
```

该设置只对当前 shell 及其子进程生效。

## 5. 验证 SimpleStorage NIXL-UCX 传输

启用后，StorageUnit 启动日志中会出现：

```text
SimpleStorage payload transfer selected: nixl-ucx device=ucx-auto gid_index=ucx-auto tls=ucx-auto
```

跨节点 PUT/GET 完成后，GET 内容应与 PUT 内容一致。该日志和数据校验只能确认
NIXL-UCX 路径可用；确认 RDMA 还需检查 payload lane，`rc_*` 表示 RDMA，TCP lane
表示使用 TCP。

## 常见问题

### RDMA 设备正常，但 NIXL-UCX 启动失败

如果 `ibv_devinfo` 能看到 RDMA 设备和活动端口，但 NIXL 初始化失败，日志中通常会有：

```text
no userspace device-specific driver found
failed to open ... libuct_ib ...
NIXL_ERR_BACKEND
```

先确认网卡对应的 provider 已安装。provider 已安装但仍然报错时，换用与系统
`rdma-core/provider` 兼容的 NIXL wheel。没有可用 wheel 时，按
[NIXL 官方源码构建说明](https://github.com/ai-dynamo/nixl#prerequisites-for-source-build-linux)
编译启用 multi-thread 和 verbs 的 UCX：

```bash
python -m pip install meson ninja pybind11 tomlkit
git clone https://github.com/openucx/ucx.git <ucx_source>
cd <ucx_source>
git checkout <nixl_supported_ucx_version>
./autogen.sh
./contrib/configure-release-mt \
  --prefix=<ucx_install_prefix> \
  --enable-shared \
  --disable-static \
  --with-verbs
make -j"$(nproc)"
make install
```

然后让 NIXL 使用这套 UCX：

```bash
git clone https://github.com/ai-dynamo/nixl.git <nixl_source>
cd <nixl_source>
python -m pip install .
meson setup build \
  -Ducx_path=<ucx_install_prefix> \
  -Dprefix=<nixl_install_prefix> \
  -Dbuildtype=release
ninja -C build
ninja -C build install
python -m pip install build/src/bindings/python/nixl-meta/nixl-*-py3-none-any.whl
```

### 找不到 `libnixl.so`

`import nixl` 时报 `libnixl.so: cannot open shared object file`，说明 NIXL 的 Python
扩展没有找到 NIXL 动态库，此时还没有进入 UCX 初始化。

先查找 wheel 中的 `libnixl.so`：

```bash
NIXL_SITE=$(python -c 'import site; print(site.getsitepackages()[0])')
find "${NIXL_SITE}" -name libnixl.so
```

如果能找到，在启动 TQ/Ray 的同一个 shell 中把所在目录加入 `LD_LIBRARY_PATH`：

```bash
NIXL_LIB_DIR=/path/to/directory/containing/libnixl.so
export LD_LIBRARY_PATH="${NIXL_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
```

如果找不到，重新安装报错中对应的 NIXL wheel。例如：

```bash
python -m pip install --no-cache-dir --force-reinstall --no-deps nixl-cu12
```
