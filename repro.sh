#!/usr/bin/env bash
# Reproduces the upstream apiserver watchCache "stuck high after burst" bug
# on a vanilla kind cluster. See README.md for full explanation.
#
# Usage:
#   ./repro.sh                          # default: 1000 widgets, 20KB body
#   COUNT=2000 PADDING=50000 ./repro.sh # bigger
#   USE_EXISTING_CLUSTER=1 ./repro.sh   # skip kind create/delete
#   SETTLE_MIN=10 ./repro.sh            # wait 10 min after delete (default 5)
#
# Outputs:
#   ./out/<timestamp>/cap.log     ring capacity over time
#   ./out/<timestamp>/rss.log     apiserver container RSS over time
#   ./out/<timestamp>/events.log  high-level milestones
#   ./out/<timestamp>/verdict.md  pass/fail + numbers

set -euo pipefail

# --- knobs --------------------------------------------------------------
CLUSTER_NAME=${CLUSTER_NAME:-cacher-leak}
COUNT=${COUNT:-1000}
PADDING=${PADDING:-20000}        # bytes of `spec.data` per widget
SETTLE_MIN=${SETTLE_MIN:-5}      # minutes to wait after delete
USE_EXISTING_CLUSTER=${USE_EXISTING_CLUSTER:-0}
KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.31.0}

# --- paths --------------------------------------------------------------
HERE=$(cd "$(dirname "$0")" && pwd)
TS=$(date -u +%Y%m%dT%H%M%SZ)
OUTDIR="$HERE/out/$TS"
mkdir -p "$OUTDIR"
log()  { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$OUTDIR/events.log"; }
fail() { echo "[$(date -u +%H:%M:%S)] FAIL: $*" | tee -a "$OUTDIR/events.log"; exit 1; }

# --- preflight ----------------------------------------------------------
command -v kubectl >/dev/null || fail "kubectl not on PATH"
command -v kind    >/dev/null || fail "kind not on PATH"
command -v docker  >/dev/null || fail "docker not on PATH"

log "params: COUNT=$COUNT PADDING=${PADDING}B SETTLE_MIN=${SETTLE_MIN}m USE_EXISTING_CLUSTER=$USE_EXISTING_CLUSTER"
log "out dir: $OUTDIR"

# --- cluster ------------------------------------------------------------
if [[ "$USE_EXISTING_CLUSTER" == "1" ]]; then
  log "using existing kube context: $(kubectl config current-context)"
  CONTAINER_NAME=""
else
  log "creating kind cluster $CLUSTER_NAME"
  kind create cluster --name="$CLUSTER_NAME" --image="$KIND_NODE_IMAGE" --wait=2m
  kind export kubeconfig --name="$CLUSTER_NAME"
  CONTAINER_NAME="${CLUSTER_NAME}-control-plane"
fi

# Resolve the apiserver container for RSS observation.
if [[ -z "${CONTAINER_NAME:-}" ]]; then
  CONTAINER_NAME=$(docker ps --format '{{.Names}}' | grep -E 'control-plane' | head -1 || true)
fi
if [[ -z "$CONTAINER_NAME" ]]; then
  log "WARN: no kind control-plane container found; RSS log will be empty"
fi

# --- background observers ----------------------------------------------
> "$OUTDIR/cap.log"
> "$OUTDIR/rss.log"

# Ring capacity (every 5s).
# Notes on parsing:
#   1. Upstream apiserver emits labels in a definition-dependent order, so
#      we must NOT require `resource="..."` to be the first label.
#   2. The cacher's watchCache metrics use the resource's full
#      `GroupResource.String()` — for built-ins like pods that's just
#      `"pods"`, but for CRDs it's `"<resource>.<group>"`. We try the
#      FQDN form first and fall back to the bare form.
metric_value() {
  # $1 = metric name; remaining args = candidate resource label values
  local name=$1; shift
  local res
  for res in "$@"; do
    local out
    out=$(echo "$metrics" | awk -v name="$name" -v res="$res" '
      $0 ~ "^" name "{" {
        if ($0 ~ ("resource=\"" res "\"")) { print $NF; exit }
      }
    ')
    if [[ -n "$out" ]]; then
      echo "$out"
      return
    fi
  done
}

(
  # Observers are logging-forever loops; don't let a single failed kubectl
  # / parse / [[ ]] AND-OR list kill the loop via the parent's `set -e`.
  set +eo pipefail
  parse_warn_dumped=0
  while true; do
    metrics=$(kubectl get --raw /metrics 2>/dev/null)
    # CRD-backed cachers label by `widgets.example.com`; built-ins by
    # bare name. metric_value tries each candidate in order.
    cap=$(metric_value  watch_cache_capacity                widgets.example.com widgets)
    inc=$(metric_value  watch_cache_capacity_increase_total widgets.example.com widgets)
    dec=$(metric_value  watch_cache_capacity_decrease_total widgets.example.com widgets)
    objs=$(metric_value apiserver_storage_objects           widgets.example.com widgets)
    echo "$(date -u +%H:%M:%S) cap=${cap:-?} inc=${inc:-?} dec=${dec:-?} objs=${objs:-?}" >> "$OUTDIR/cap.log"
    # First time we fail to parse cap, dump a sample of the raw metrics so
    # next run we can see what format upstream is emitting.
    if [[ -z "$cap" && $parse_warn_dumped -eq 0 && -n "$metrics" ]]; then
      {
        echo "# raw /metrics sample taken at $(date -u +%H:%M:%S)"
        echo "# couldn't parse watch_cache_capacity{resource=\"widgets\"}"
        echo "# dumping any line containing 'watch_cache_capacity', 'widgets', or storage_objects:"
        echo "$metrics" | grep -E 'watch_cache_capacity|widgets|apiserver_storage_objects'
      } > "$OUTDIR/metrics-sample.txt" 2>/dev/null
      parse_warn_dumped=1
    fi
    sleep 5
  done
) &
CAP_PID=$!

# apiserver container RSS (every 5s)
if [[ -n "$CONTAINER_NAME" ]]; then
  (
    set +eo pipefail
    while true; do
      mem=$(docker stats --no-stream --format '{{.MemUsage}}' "$CONTAINER_NAME" 2>/dev/null | awk '{print $1}')
      echo "$(date -u +%H:%M:%S) container=$CONTAINER_NAME mem=$mem" >> "$OUTDIR/rss.log"
      sleep 5
    done
  ) &
  RSS_PID=$!
else
  RSS_PID=""
fi

cleanup() {
  kill "$CAP_PID" 2>/dev/null || true
  [[ -n "$RSS_PID" ]] && kill "$RSS_PID" 2>/dev/null || true
}
trap cleanup EXIT

# --- workload -----------------------------------------------------------
log "applying widget CRD"
kubectl apply -f "$HERE/widget-crd.yaml" >/dev/null
kubectl wait --for=condition=Established crd/widgets.example.com --timeout=60s >/dev/null
log "CRD established"

# Brief baseline window
log "baseline: 30s pre-burst observation"
sleep 30

# --- burst create -------------------------------------------------------
log "BURST CREATE: $COUNT widgets, ${PADDING}B each"
# Build the manifest stream and apply once. Server-side apply tolerates dup runs.
# Using yq/printf-based generation keeps memory bounded on the client side.
PAD=$(python3 -c "print('x'*$PADDING)")
{
  for i in $(seq 1 "$COUNT"); do
    cat <<EOF
apiVersion: example.com/v1
kind: Widget
metadata:
  name: widget-$i
spec:
  data: "$PAD"
---
EOF
  done
} | kubectl apply -f - >/dev/null
log "BURST CREATE done"

# --- settle a bit -------------------------------------------------------
log "post-create settle: 30s"
sleep 30
# Find the max integer cap= seen in the log. Tokens that are not pure
# integers (e.g. "?") are skipped so peak_cap is always numeric (0 if none).
peak_cap=$(awk '
  {
    for (i = 1; i <= NF; i++) {
      if ($i ~ /^cap=[0-9]+$/) {
        v = $i + 0  # strip cap= prefix via numeric coercion
        sub(/cap=/, "", $i)
        if ($i + 0 > m) m = $i + 0
      }
    }
  }
  END { print (m ? m : 0) }
' "$OUTDIR/cap.log")
log "peak ring capacity observed: $peak_cap"

# --- burst delete -------------------------------------------------------
log "BURST DELETE: kubectl delete widget --all"
kubectl delete widget --all --wait=true >/dev/null
log "BURST DELETE done"

# --- post-cleanup settle ------------------------------------------------
log "post-delete settle: ${SETTLE_MIN} minute(s)"
sleep "$((SETTLE_MIN * 60))"

# --- verdict ------------------------------------------------------------
final=$(tail -1 "$OUTDIR/cap.log")
extract() {
  # $1 = key (cap|objs|dec|inc), reads from $final
  echo "$final" | awk -v key="$1" '
    {
      for (i = 1; i <= NF; i++) {
        if (index($i, key "=") == 1) {
          v = substr($i, length(key) + 2)
          if (v ~ /^[0-9]+$/) print v; else print ""
          exit
        }
      }
    }
  '
}
final_cap=$(extract cap)
final_objs=$(extract objs)
final_dec=$(extract dec)

log "final state: $final"

is_num() { [[ "$1" =~ ^[0-9]+$ ]]; }

# `watch_cache_capacity_decrease_total` is a Prometheus CounterVec — its
# line for a given resource label only appears in /metrics AFTER the first
# .Inc() call. When the shrink branch never fires, the line never exists,
# so an absent value here actually means "0 shrinks have happened" =
# strongest possible signal of the bug. We treat a missing/non-numeric
# dec as 0 for verdict purposes, but only if cap and inc were both parsed
# (which proves the cacher exists and we just didn't see a shrink line).
dec_num=$final_dec
if ! is_num "$final_dec"; then
  if is_num "$final_cap"; then
    dec_num=0
    final_dec_note="(missing from /metrics — counter never incremented)"
  fi
fi

verdict_file="$OUTDIR/verdict.md"
{
  echo "# watchCache repro verdict"
  echo
  echo "- params: COUNT=$COUNT PADDING=${PADDING}B SETTLE=${SETTLE_MIN}m"
  echo "- peak ring capacity:  ${peak_cap:-?}"
  echo "- final ring capacity: ${final_cap:-?}"
  echo "- final etcd objects:  ${final_objs:-?}"
  echo "- shrink counter:      ${final_dec:-?} ${final_dec_note:-}"
  echo
  if ! is_num "$final_cap" || ! is_num "$final_objs" || ! is_num "$dec_num"; then
    echo "## INCONCLUSIVE"
    echo
    echo "Could not parse final metrics. Check $OUTDIR/cap.log for raw samples"
    echo "and $OUTDIR/metrics-sample.txt for the raw /metrics dump."
  elif (( final_cap > 200 )) && (( final_objs < 5 )) && (( dec_num == 0 )); then
    echo "## BUG REPRODUCED"
    echo
    echo "Ring stayed at $final_cap with $final_objs live objects. No shrink."
    echo "Shrink counter ${final_dec:-?} ${final_dec_note:-= 0}."
  elif (( dec_num > 0 )); then
    echo "## FIX APPEARS PRESENT"
    echo
    echo "Shrink counter advanced to $dec_num — ring resized down on idle."
  else
    echo "## INCONCLUSIVE"
    echo
    echo "Final cap=$final_cap, objs=$final_objs, dec=$final_dec — check cap.log."
  fi
} > "$verdict_file"

log "verdict written to $verdict_file"
cat "$verdict_file"

# --- teardown -----------------------------------------------------------
if [[ "$USE_EXISTING_CLUSTER" != "1" ]]; then
  log "deleting kind cluster $CLUSTER_NAME"
  kind delete cluster --name="$CLUSTER_NAME"
fi
log "done"
