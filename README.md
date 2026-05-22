# watchCache "stuck high after burst" reproducer

Reproduces the upstream kube-apiserver watchCache bug we identified during
the kcp memory investigation (see
[`../memory-test-fix-3/PLAN.md`](../memory-test-fix-3/PLAN.md) for the
full chain of evidence). The bug is **not kcp-specific** — it lives in
`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go` upstream.
This repro runs on a vanilla kind cluster and shows the behavior with no
kcp components in the picture.

## The bug

`watchCache.resizeCacheLocked()` is the only path that shrinks the ring
buffer. It's called from `updateCache()` on each event, and both its grow
and shrink branches require `isCacheFullLocked()` to be true. After a
burst-then-quiesce workload:

- Ring is inflated (e.g. 1,600 slots)
- Burst ends; no further events arrive on that resource
- `updateCache()` is never called → resize never runs → ring stays at
  high-water indefinitely

Each ring slot pins a deep-copied `watchCacheEvent` (the full resource
body + previous version + labels/fields). For a 1,600-slot ring with
~20 KB widgets, that's ~30 MB stuck on a single resource — multiply by
the number of bursty resources for the production-scale impact.

See "What plain kube users hit" in
[`../memory-test-fix-3/PLAN.md`](../memory-test-fix-3/PLAN.md) for the
discussion of when this surfaces in real clusters.

## What the reproducer does

1. Creates a kind cluster (or uses an existing one).
2. Applies a minimal CRD (`widgets.example.com`).
3. Bursts `$COUNT` widgets with `$PADDING` bytes of body each.
4. Waits ~30s for the watchCache to grow and settle.
5. Deletes all widgets with `kubectl delete widget --all`.
6. Waits `$SETTLE_MIN` minutes for would-be equilibration.
7. Asserts the ring is still at high capacity with `apiserver_storage_objects ≈ 0`.

Throughout the run, two background loggers capture
`watch_cache_capacity{resource="widgets"}` and the apiserver container's
RSS into `out/<timestamp>/`.

## Run

```bash
./repro.sh
```

Defaults: 1000 widgets, 20 KB body, 5-minute settle. Tunable:

```bash
COUNT=2000   PADDING=50000  SETTLE_MIN=10  ./repro.sh   # bigger
USE_EXISTING_CLUSTER=1 ./repro.sh                       # don't create/destroy kind
KIND_NODE_IMAGE=kindest/node:v1.31.0 ./repro.sh          # different node image
```

The script auto-creates and tears down its own kind cluster unless
`USE_EXISTING_CLUSTER=1` is set.

## Output

`out/<UTC timestamp>/` contains:

| file | content |
|---|---|
| `cap.log` | per-5s sample of `watch_cache_capacity{widgets}` + grow/shrink counters + live object count |
| `rss.log` | per-5s sample of the apiserver container memory (via `docker stats`) |
| `events.log` | high-level milestones (CRD applied, burst start/end, etc.) |
| `verdict.md` | summary + `BUG REPRODUCED` / `FIX APPEARS PRESENT` / `INCONCLUSIVE` |

## Expected output with vanilla kube (bug present)

```
peak ring capacity:  1600
final ring capacity: 1600
final etcd objects:  0
shrink counter:      0   (0 = bug present, ≥1 = shrink fired)

## BUG REPRODUCED

Ring stayed at 1600 with 0 live objects. No shrink.
```

`rss.log` shows apiserver RSS step up during the burst and stay there.

## Expected output with the fix

If a `ShrinkIdleLoop` patch (or equivalent) is built into the apiserver:

```
peak ring capacity:  1600
final ring capacity: 100      (or wherever the floor settled)
final etcd objects:  0
shrink counter:      4        (or however many halve steps fired)

## FIX APPEARS PRESENT
```

`rss.log` shows the apiserver RSS step up during burst then step back
down over ~5–10 minutes after delete.

## Making the signal bigger

The default 1000 × 20 KB ≈ 20 MB of stuck retention is enough to
demonstrate the bug but not dramatic on a multi-GB cluster. To make it
unambiguous:

- More resources: define multiple CRDs (gizmos, gadgets, etc.) and burst
  each one. Per-cacher rings stack additively.
- Fatter bodies: `PADDING=100000` puts ~100 KB per object.
- Tighter memory: in `kind`, set the control-plane container's memory
  limit (via a custom kind config) to e.g. 512 MiB. Two consecutive
  bursts will OOM-kill the apiserver — kubelet restarts it, and the
  rings reset to lowerBound (the de-facto workaround in production).

## Files

- [`widget-crd.yaml`](widget-crd.yaml) — minimal CRD
- [`repro.sh`](repro.sh) — orchestrator
- [`README.md`](README.md) — this file
- `out/<ts>/` — per-run artifacts (git-ignored)
