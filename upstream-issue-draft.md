# Draft: upstream issue to file at kubernetes/kubernetes

This is a draft for review. NOT YET POSTED.

---

## Title

`watchCache ring capacity not reduced after burst when event rate drops below the fullness threshold (resize is gated on isCacheFullLocked())`

## Body

/kind bug
/sig api-machinery

### What happened

`watchCache.resizeCacheLocked()` is the only path that can shrink the watch cache ring, and it is called exclusively from `updateCache()` — i.e. only when a new event arrives for the resource. Both its grow and shrink branches additionally require `isCacheFullLocked()` to be true:

[`staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go`](https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache.go) (current master):

```go
func (w *watchCache) resizeCacheLocked(eventTime time.Time) {
    if w.isCacheFullLocked() && eventTime.Sub(w.cache[w.startIndex%w.capacity].RecordTime) < w.eventFreshDuration {
        capacity := min(w.capacity*2, w.upperBoundCapacity)
        if capacity > w.capacity {
            w.doCacheResizeLocked(capacity)
        }
        return
    }
    if w.isCacheFullLocked() && eventTime.Sub(w.cache[(w.endIndex-w.capacity/4)%w.capacity].RecordTime) > w.eventFreshDuration {
        capacity := max(w.capacity/2, w.lowerBoundCapacity)
        if capacity < w.capacity {
            w.doCacheResizeLocked(capacity)
        }
        return
    }
}
```

Under a "burst then quiesce" workload (high create/delete rate on a resource type, followed by inactivity), this combination produces a permanent retention:

1. Burst inflates the ring via the grow branch up to e.g. `capacity = 1600` or higher.
2. Burst ends. Ring is no longer full — `endIndex - startIndex < capacity`.
3. Few or no further events arrive on that resource.
4. `updateCache()` is therefore not called → `resizeCacheLocked` never runs.
5. Even if a single event arrives, `isCacheFullLocked()` is false on entry → both branches are skipped → ring stays at high-water capacity indefinitely.

Each ring slot pins a deep-copied `watchCacheEvent` containing the full `Object`, `PrevObject`, labels, and fields of the resource at that revision. For workloads with reasonably-sized objects (say 20 KB each), a single ring stuck at 1600 holds ~30 MB indefinitely; multiplied across the dozens of resource types in a busy cluster, this commonly reaches several hundred MB to >1 GB of post-burst stuck retention.

### What you expected to happen

The original design intent in #90058 (the issue that drove the dynamic-cache PR #90091) is explicit:

> "decrease size by 2x when the X minutes history would fit into half of it"

This intent is satisfied today **only while events keep arriving fast enough to keep the ring full**. In all other cases — including the realistic and common "we just finished a workload" case — the ring is stuck at peak capacity.

The expected behavior is that after the configured freshness window (`eventFreshDuration`, default 75 s) has elapsed past the most-recent event in the ring, the ring should start halving until it reaches `lowerBoundCapacity`. This should happen regardless of whether new events are still arriving.

### How to reproduce

A fully self-contained reproducer (vanilla `kindest/node:v1.31.0` + one CRD, no extra components) is available at <link to public mirror — TBD>. Summary of what it does:

1. `kind create cluster`
2. Apply a minimal `widgets.example.com` CRD (cluster-scoped, single `spec.data: string` field).
3. Burst-create 1000 widgets via `kubectl apply -f -`, each with ~20 KB `spec.data` body.
4. Wait 30 s for the watchCache to settle.
5. `kubectl delete widget --all`; wait for all deletes to complete.
6. Sleep 5 minutes (already enough — see settle behavior in our 60-minute k8s run, which showed `T3 ≈ T4 ≈ T5`).
7. Read `kubectl get --raw /metrics`.

Actual measurements from one run on `kindest/node:v1.31.0`:

```text
peak ring capacity (post-create, +30s settle):  1600
final ring capacity (+5min post-delete):        3200   ← grew during DELETE burst, never shrank
live etcd objects (apiserver_storage_objects{resource="widgets.example.com"}): 0
watch_cache_capacity_increase_total{resource="widgets.example.com"}: 5
watch_cache_capacity_decrease_total{resource="widgets.example.com"}: <absent from /metrics>
```

Two notable observations:

- **The DELETE burst pushed the ring higher than the CREATE burst** (1600 → 3200). The same dynamic resize path that doubles on grow does fire during a cleanup-event burst — confirming the cacher is healthy, just not shrinking afterwards.
- **`watch_cache_capacity_decrease_total{resource="widgets.example.com"}` does not appear in `/metrics` at all.** Prometheus `CounterVec` emits a series for a label combination only after the first `.Inc()` call. Its absence means the shrink branch of `resizeCacheLocked` literally never executed for this resource — strongest possible evidence of the bug, beyond just "value is 0".

apiserver container RSS shows the same step-up pattern and never recovers without a process restart.

### Anything else we need to know?

#### Related issues / PRs

This appears to be the first explicit identification of the resize-gating root cause. Related symptom reports without root-cause analysis:

- #102259 — "Kubernetes API server memory usage spikes when stopping a large number of watches" (CLOSED, same symptom class)
- #98423 — "Kube-apiserver high memory usage on pending pods storm" (CLOSED)
- #97142 — "kube-apiserver consume high memory until it is exhausted" (CLOSED)
- #30025 — "Bulk deletes are resource intensive on caches" (OPEN, ancient — exactly this pattern)
- #65954 — "apiserver cannot recover after restarting … in large scale cluster" (CLOSED, related but cache repopulation, not retention)

Design / implementation pair where the gap was introduced:

- #90058 — design issue with the explicit "decrease … when … would fit into half" intent
- #90091 — implementation PR (merged in v1.19), which only addressed the grow side fully and gated shrink behind `isCacheFullLocked()`

Adjacent in-flight work that touches the same file:

- #129205 (MERGED v1.33) — history window based on request timeout
- #127091 (CLOSED, 2025) — slim down per-slot event payload via cachingobject
- #137109 (OPEN) — string interning to cut cache memory
- #90179 (OPEN, lavalamp) — broader rethink "More memory efficient watch cache"

#### Proposed fix sketch

A periodic (time-driven) shrink that bypasses the `isCacheFullLocked()` gate. Logic mirrors the existing shrink condition's intent without requiring a fresh event to drive it:

```go
// New method on watchCache. Launched by the cacher as a goroutine.
func (w *watchCache) shrinkIdleLoop(stopCh <-chan struct{}) {
    ticker := w.clock.NewTicker(w.eventFreshDuration)
    defer ticker.Stop()
    for {
        select {
        case <-stopCh:
            return
        case <-ticker.C():
            w.Lock()
            w.maybeShrinkOnIdleLocked(w.clock.Now())
            w.Unlock()
        }
    }
}

func (w *watchCache) maybeShrinkOnIdleLocked(now time.Time) {
    if w.capacity <= w.lowerBoundCapacity {
        return
    }
    eventCount := w.endIndex - w.startIndex

    var target int
    switch {
    case eventCount == 0:
        target = w.lowerBoundCapacity
    default:
        oldest := w.cache[w.startIndex%w.capacity]
        if oldest == nil {
            return
        }
        if now.Sub(oldest.RecordTime) <= w.eventFreshDuration {
            return // oldest still fresh; respect history guarantee
        }
        target = max(w.capacity/2, w.lowerBoundCapacity)
        if target < eventCount {
            target = eventCount // never evict live events on shrink
        }
    }
    if target < w.capacity {
        w.shrinkRingLocked(target)
    }
}

// Safe shrink-only variant of doCacheResizeLocked for non-full rings.
func (w *watchCache) shrinkRingLocked(newCapacity int) {
    if newCapacity >= w.capacity {
        return
    }
    eventCount := w.endIndex - w.startIndex
    if newCapacity < eventCount {
        w.startIndex = w.endIndex - newCapacity
        w.removedEventSinceRelist = true
    }
    newCache := make([]*watchCacheEvent, newCapacity)
    for i := w.startIndex; i < w.endIndex; i++ {
        newCache[i%newCapacity] = w.cache[i%w.capacity]
    }
    w.cache = newCache
    metrics.RecordsWatchCacheCapacityChange(w.groupResource, w.capacity, newCapacity)
    w.capacity = newCapacity
}
```

Properties:

- **No behavior change for healthy clusters.** If events keep arriving and the ring stays effectively full, the ticker tick finds the oldest event fresh and no-ops. The existing event-driven resize remains the primary path.
- **Reclaims memory on idle.** After workload quiesce the ring halves every `eventFreshDuration` (default 75 s) until it reaches `lowerBoundCapacity` (default 100). Total drain time from `defaultUpperBoundCapacity = 102400` to floor: ~10 ticks ≈ ~12 minutes.
- **No event loss.** `target ≥ eventCount` ensures we never evict events that are still in the ring during the shrink path.
- **Lock contention is bounded.** One write-lock acquisition per `eventFreshDuration` per cacher. Negligible.

Tests to add: unit tests for `maybeShrinkOnIdleLocked` with a fake clock across (empty / fresh / stale-oldest) cases; and for `shrinkRingLocked` ensuring it preserves all events for `newCap >= eventCount`.

### Environment

- Kubernetes version verified: **v1.31.0** (`kindest/node:v1.31.0`). The resize logic in `watch_cache.go` is unchanged since #90091 merged in v1.19, so this affects **v1.19.0 through current master**.
- Workload: see reproducer above. No add-ons, no operators — vanilla kind, single CRD.

---
