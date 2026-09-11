# BENCHMARKS

Measured, reproducible numbers — not textbook estimates. Each entry says
how it was produced; re-run the referenced test yourself to reproduce.

---

## M2 §6.6 — the fsync central question

**Question:** how much throughput/latency does `flush.messages=1` (fsync
after every single append) actually cost, compared to a batched policy, on
this machine?

**Method:** [`FsyncBenchmarkTest`](src/test/java/com/advaith/broker/log/FsyncBenchmarkTest.java)
appends 500 real, CRC-valid RecordBatches (1 record each) to a
`PartitionLog` under each policy, timing each `append()` call individually
with `System.nanoTime()`. Latencies are sorted to compute p50/p99;
throughput is total appends divided by total wall-clock time. No JMH —
PRD §3 has no dependency budget for a benchmarking framework, so this is a
plain wall-clock harness. Run it yourself with:

```bash
mvn test -Dtest=FsyncBenchmarkTest
```

**Results** (this machine, this run — re-run to get your own):

| Policy | Throughput | p50 latency | p99 latency |
|---|---|---|---|
| `flush.messages=1` (every record) | ~900-940 appends/sec | ~945-950µs | ~2.4-2.7ms |
| `flush.messages=200` (batched) | ~109,000-116,000 appends/sec | ~0-1µs | ~16-20µs |

**Batched flushing is roughly 117-127x the throughput of per-record fsync
on this machine**, and per-append p50 latency drops by close to three
orders of magnitude (µs vs. sub-ms) when `fsync` isn't on the critical
path of every single append.

**What this number means, and doesn't mean:**
- It is **not** "fsync is slow, therefore always batch." It's "this is the
  concrete price of the strongest single-broker durability guarantee M2
  can offer, on this specific machine." A different disk (spinning vs.
  SSD vs. NVMe), a different filesystem, or a different OS's I/O scheduler
  would all move the absolute numbers — the qualitative shape (fsync is
  the dominant cost when it's on the hot path) is the durable takeaway,
  not the exact multiplier.
- `flush.messages=1` is still M2's **default** in `broker.properties` —
  the safest choice is the right starting point; trading it away for
  throughput should be a deliberate, informed choice a deployer makes,
  not an accident of an unexamined default.
- This measures **only** the storage layer's append cost — it says
  nothing about network/protocol overhead, which M1's own throughput
  ceiling (see `STUDY_GUIDE.md` §7, gate Q15) already covers separately.
- **This environment is a sandboxed/containerized dev machine**, not
  bare-metal hardware with a known disk type — worth saying explicitly
  before quoting these numbers as if they were a production SLA. The
  honest claim is "I measured a ~100x-order-of-magnitude gap between
  per-record and batched fsync, using a real, reproducible harness, on
  the machine I had" — which is exactly the PRD's actual bar ("measured
  figures you generated yourself are the only kind you can fully defend",
  §6/M5), not "these are Kafka's official numbers."
