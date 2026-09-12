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

---

## M4 §8.8 — leader failover time (measured, real 3-broker cluster)

**Setup:** 3 separate OS processes (`config/cluster/broker-{0,1,2}.
properties`, replication factor 3, `leader.lease.timeout.ms=6000`,
`leader.lease.renew.interval.ms=2000`), a real `kafka-python` producer
sending continuously with `acks=all`, `kill -9` issued against the
current leader of `test-0` mid-stream, wall-clock timestamped at the
instant `os.kill()` was called and compared against when `Metadata`
first reported a different leader.

| Run | Elapsed from `kill -9` to new leader visible in `Metadata` |
|---|---|
| 1 | 7.09s |
| 2 | 6.83s |
| 3 (post grace-period widening, §15.5 bug 4) | 6.83s |

**What this measures, and doesn't:** the dominant term is the configured
`leader.lease.timeout.ms` (6000ms) itself — a broker only concludes a
peer is dead after that many milliseconds of failed contact — plus a
smaller, variable tail from the `PeerReplicator` retry-loop's own
granularity (a ~1s backoff between reconnect attempts) and this specific
sandboxed environment's process-kill/TCP-teardown latency. The number to
actually reason about isn't "6.8-7.1 seconds" as some universal constant
— it's "failover time is bounded above by `leader.lease.timeout.ms` plus
one retry-loop cycle," which is a dial the operator controls, not a fixed
property of the mechanism. Setting it lower trades faster failover for a
greater chance of promoting over a merely-slow-not-dead leader (and, per
§15.5's own bug 4, needs the startup-grace-period multiplier widened to
match, or ordinary staggered startup starts looking like a failure too).

This is the one M4 number worth quoting — the acceptance criterion itself
(§8.8.2) only requires "within `leader.lease.timeout.ms`" as a bound, not
a specific value, so demonstrating the actual measured number (rather
than asserting it in prose) is what makes this defensible.

---

## M5 §9.2 — chaos test: zero acknowledged-message loss under a real leader kill

**Setup:** [`ChaosFailoverTest`](src/test/java/com/advaith/broker/chaos/ChaosFailoverTest.java)
— 3 real broker OS processes (fresh temp data dirs, free ports, fast-but-real
lease timing: `leader.lease.timeout.ms=1500`), a real hand-rolled producer
sending 200 records with `acks=all` over the real wire protocol, waiting for
the ISR to actually reach all 3 replicas before starting the timed load
(see JOURNAL.md, 2026-09-12, for why that wait is itself load-bearing), then
`SIGKILL`-ing the current leader after the 80th acknowledgment and
continuing production against whichever broker Metadata reports next.
After the run, the entire partition is read back from offset 0 and every
acknowledged (offset, value) pair is checked for exact presence.

**Result, run repeatedly (PRD §9.6 criterion 1 — "run it several times"):**

| Run | Outcome | Acknowledged offsets verified | Approx. failover-to-first-post-kill-ack |
|---|---|---|---|
| 1-8 (this session) | **PASS**, all 8 | 200/200 every time | ~4-8s (bounded by `leader.lease.timeout.ms` + retry-loop granularity, same shape as M4's own §8.8 measurement) |

Reproduce it yourself:
```bash
mvn -DskipTests package
mvn test -Dtest=ChaosFailoverTest
```
(needs the jar built first — `mvn test` alone runs before `package` in the
Maven lifecycle, so a from-clean `mvn test` skips this test via a JUnit
`Assumption` rather than failing.)

**What this proves, and what it explicitly doesn't:** every offset the
producer received an `acks=all` acknowledgment for survived the leader's
death, at the exact offset, with the exact value, in order — that is the
literal, checkable content of "zero acknowledged-message loss." It does
**not** claim exactly-once delivery (this project has no idempotent
producer, PRD §2 — a message that failed with a connection reset right as
its ack would have arrived, then got retried, can appear twice at two
different offsets; that's a duplicate of *unacknowledged* data, never a
loss of acknowledged data), and it does not claim safety against a
correlated failure (all 3 replicas failing at once) or the network-
partition-driven split-brain window §15.4 already names as a deliberate,
understood cost of a lease over a real quorum.

**A real bug this test found before it was passing repeatably** (see
JOURNAL.md, 2026-09-12, for the full narration): a fast `acks=all` producer
loop can rack up acknowledgments before either follower's replication link
has completed even one round trip against a brand-new leader — meaning the
ISR those early acks were computed against was, for real, size 1, no
stronger a guarantee than `acks=1`, through no fault of the replication
code. The fix was to the *test*, not the broker: wait for the ISR to
reach all 3 replicas before trusting any acknowledgment's strength, the
same discipline a real operator would apply before chaos-testing a
still-forming cluster. This also surfaced a genuine, now-documented scope
gap: this project has no `min.insync.replicas`-equivalent knob, so
`acks=all` is only ever as strong as whatever the current ISR happens to
be (see STUDY_GUIDE.md §17).

---

## M5 §9.3 — whole-system throughput/latency benchmark, to saturation

**Setup:** [`ThroughputBenchmarkTest`](src/test/java/com/advaith/broker/benchmark/ThroughputBenchmarkTest.java)
— one embedded broker (real `NetworkServer`, real sockets to localhost —
unlike the chaos test, a benchmark doesn't need separate OS processes,
only the chaos test's independent-failure premise does), 100-byte values,
`acks=1`, a **batched** flush policy (`log.flush.interval.messages=1000`
— deliberately NOT M2's per-record default, so this measures the
network/protocol-layer ceiling rather than re-measuring M2's own
already-documented ~120-170x fsync cost). Load is driven by an increasing
number of concurrent persistent connections (1, 2, 4, 8, 16, ...), each
hammering `Produce` in a tight loop for 1.5s; throughput is total records
divided by wall time, until the gain over the previous level falls below
10% (PRD §9.3's "the actual ceiling, found empirically, not assumed").

Reproduce it yourself:
```bash
mvn test -Dtest=ThroughputBenchmarkTest
```

**Results** (this machine, this run — re-run to get your own):

| Concurrency | Throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| 1 | ~18,000-19,500 records/sec | ~40µs | ~100µs | ~180-230µs |
| 2 | ~49,000-51,000 records/sec | ~30µs | ~65-68µs | ~110-122µs |
| 4 | ~56,000-58,400 records/sec | ~62µs | ~89-99µs | ~150-166µs |
| 8 (saturation) | **~57,000-59,200 records/sec** | ~107-112µs | ~179-191µs | ~330-460µs |

**Saturation was reached at 8 concurrent connections**, consistently
across repeated runs, at roughly **57,000-59,000 records/sec**.

**What the broker was spending its time on at that point — profiled, not
guessed:** a hand-rolled sampling profiler (this project has no profiler
dependency budget, so every 2ms it snapshots the server's own single
selector thread's real call stack — the same principle a real sampling
profiler like async-profiler or JFR uses) shows, at the saturation level,
the overwhelming majority of samples inside `SocketDispatcher.write0`
(sending responses) and `SocketDispatcher.read0` (reading requests), with
a real but secondary share in `UnixFileDispatcherImpl.force0`/`pwrite0`
(fsync/write — batched, so present but not dominant) and `EPoll.ctl`
(interest-set churn as connections' read/write readiness flips).

**Is this the same bottleneck M1's gate-15 answer predicted?** Essentially
yes, but more specific than "single-threaded" alone predicted: gate 15
named the single selector thread itself as the structural ceiling (no
request-level parallelism, full stop). This benchmark confirms that
prediction AND sharpens it — the thread isn't CPU-bound doing encoding,
CRC computation, or log-append bookkeeping at saturation; it's spending
essentially all of its time in the raw socket I/O syscalls themselves
(`write`/`read`), meaning the ceiling here is "how many small
request/response round trips one OS thread can drive through the kernel's
socket layer per second" — a slightly different, and more actionable,
statement than "it's single-threaded" alone. A real next step (explicitly
not built — see PRD §2/non-goals scope) would be batching multiple
responses per `write()` call or moving to a small fixed worker pool behind
the same selector, trading this project's simplicity for exactly the
throughput this profile shows is being spent on syscall overhead, not
computation.
