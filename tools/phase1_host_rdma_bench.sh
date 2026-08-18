#!/usr/bin/env bash
set -euo pipefail

# Run on a control machine after both remote shells have been prepared with
# the intended UCX runtime and node-local UCX configuration.

: "${TQ_RDMA_SERVER_SSH:?set TQ_RDMA_SERVER_SSH}"
: "${TQ_RDMA_CLIENT_SSH:?set TQ_RDMA_CLIENT_SSH}"
: "${TQ_RDMA_SERVER_IP:?set TQ_RDMA_SERVER_IP}"
: "${TQ_RDMA_SERVER_DEVICE:?set TQ_RDMA_SERVER_DEVICE}"
: "${TQ_RDMA_CLIENT_DEVICE:?set TQ_RDMA_CLIENT_DEVICE}"
: "${TQ_RDMA_GID_INDEX:?set TQ_RDMA_GID_INDEX}"

transport=${1:-all}
case "$transport" in
  ucx|write|read|all) ;;
  *) echo "usage: $0 {ucx|write|read|all}" >&2; exit 2 ;;
esac

sizes=(65536 1048576 16777216)
run_id="$$"

run_ucx() {
  local size=$1 port=$2 log="/tmp/tq-phase1-ucx-${run_id}-${size}.log"
  ssh "$TQ_RDMA_SERVER_SSH" "rm -f '$log'; nohup ucx_perftest -p '$port' -t tag_bw -s '$size' -n 200 >'$log' 2>&1 &"
  sleep 3
  ssh "$TQ_RDMA_CLIENT_SSH" "timeout 90 ucx_perftest '$TQ_RDMA_SERVER_IP' -p '$port' -t tag_bw -s '$size' -n 200"
  ssh "$TQ_RDMA_SERVER_SSH" "grep -E 'Version|cfg#|Final:|ERROR' '$log' || true"
}

run_verbs() {
  local kind=$1 size=$2 port=$3 log="/tmp/tq-phase1-${kind}-${run_id}-${size}.log"
  ssh -tt "$TQ_RDMA_SERVER_SSH" "/usr/bin/ib_${kind}_bw -d '$TQ_RDMA_SERVER_DEVICE' -i 1 -x '$TQ_RDMA_GID_INDEX' -s '$size' -n 200 -p '$port'" >"$log" 2>&1 &
  local server_ssh_pid=$!
  sleep 3
  ssh "$TQ_RDMA_CLIENT_SSH" "timeout 90 /usr/bin/ib_${kind}_bw '$TQ_RDMA_SERVER_IP' -d '$TQ_RDMA_CLIENT_DEVICE' -i 1 -x '$TQ_RDMA_GID_INDEX' -s '$size' -n 200 -p '$port'"
  wait "$server_ssh_pid" || true
  grep -E 'Connection type|Link type|GID index|BW average' "$log" || true
}

idx=0
for size in "${sizes[@]}"; do
  if [[ "$transport" == ucx || "$transport" == all ]]; then
    run_ucx "$size" $((29600 + idx))
  fi
  if [[ "$transport" == write || "$transport" == all ]]; then
    run_verbs write "$size" $((29700 + idx))
  fi
  if [[ "$transport" == read || "$transport" == all ]]; then
    run_verbs read "$size" $((29800 + idx))
  fi
  idx=$((idx + 1))
done
