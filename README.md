# SwarmFS

**Network-Aware P2P File Distribution and Adaptive Peer Scheduling**

> *How can a P2P file distribution system dynamically select peers and pieces using observed network conditions to improve transfer performance and resilience?*

SwarmFS is a from-scratch P2P file distribution system built as a Computer Networks project. The file transfer is the infrastructure. The network-aware piece scheduler is the research contribution.

---

## What makes this different from a torrent client

A BitTorrent client answers:

> "Who has this piece?"

SwarmFS asks:

> "Who should I get this piece from **right now**?"

The scheduler scores each peer continuously using observed RTT, throughput, loss rate, stability, and current load, then assigns pieces to the best available peer. Under degraded network conditions (latency injection, packet loss, bandwidth caps), the adaptive scheduler completes downloads faster and with fewer failures than random peer selection.

---

## Architecture

```
Discovery / Tracker
       │
       ├── Peer A (seeder)
       ├── Peer B (seeder + partial leecher)
       └── Peer C (leecher)
              │
       ┌──────┴──────────┐
       │  Swarm State    │   availability per piece, rarest-first order
       └──────┬──────────┘
              │
       ┌──────┴──────────┐
       │   Downloader    │   N=4 parallel workers, one persistent conn per peer
       └──────┬──────────┘
              │
       ┌──────┴──────────┐
       │   Scheduler     │   Score(peer) = 0.35·tput + 0.30·rtt + 0.25·loss + 0.05·stab − 0.05·load
       └──────┬──────────┘
              │
       ┌──────┴──────────┐
       │    Monitor      │   EWMA RTT/throughput/loss per peer, passive + active probes
       └─────────────────┘
```

### Wire protocol

Length-prefixed JSON over TCP: `[4-byte big-endian length][JSON message]`

All message types: `REGISTER`, `PEER_LIST_REQ`, `PEER_LIST`, `HAVE`, `HEARTBEAT`, `LEAVE`, `ACK`, `HANDSHAKE`, `REQUEST`, `PIECE`, `MANIFEST_REQ`, `MANIFEST`, `PROBE`, `PROBE_ACK`, `BITFIELD_UPDATE`.

### Piece system

Files are split into 256 KB pieces. Each piece has a SHA-256 hash stored in the manifest. Every received piece is verified before being written to disk. The manifest is transferred first so the leecher knows the full shape of the download before requesting any pieces.

---

## Repository layout

```
cmd/
  tracker/      tracker binary
  peer/         peer binary (seed + leech subcommands)
  experiment/   experiment runner — produces results CSV
  analyze/      reads CSV, prints comparison table
internal/
  protocol/     wire format — messages, encoder, bitfield
  piece/        chunker, verifier, manifest, store
  tracker/      registry, TCP server
  peer/         node, seeder, leecher, config
  swarm/        availability state, rarest-first scheduling
  transfer/     parallel downloader, manifest fetch
  monitor/      per-peer EWMA stats (RTT, throughput, loss)
  scheduler/    scoring function, peer selector
pkg/
  metrics/      event log, CSV export
tests/
  integration/  end-to-end TCP tests
  scenarios/    experiment runner + scenario configs
benchmarks/     BenchmarkScore, BenchmarkBestPeer, BenchmarkRarestMissing
scripts/
  netem/        tc netem helpers for controlled experiments
```

---

## Build

```bash
# requires Go 1.22+
go build -mod=vendor ./cmd/tracker ./cmd/peer ./cmd/experiment ./cmd/analyze
```

Or with make:

```bash
make build
```

---

## Usage

### Seed a file

```bash
./tracker --addr :7000 &

./peer seed \
  --file largefile.bin \
  --listen :9001 \
  --tracker localhost:7000
```

### Download a file

```bash
./peer leech \
  --hash <fileHash printed by seed> \
  --listen :9002 \
  --tracker localhost:7000 \
  --out ./downloaded.bin
```

### Run the experiment suite

```bash
# baseline — no network conditions
make experiment

# with latency (requires root + iproute2)
sudo scripts/netem/set_latency.sh 100
make experiment
sudo scripts/netem/reset.sh

# view comparison table
make analyze
```

---

## Experiment design

The experiment runs five scenarios, each comparing **static** (first-available peer selection) vs **adaptive** (scored peer selection):

| Scenario | Condition |
|---|---|
| `baseline` | clean loopback |
| `latency_100ms` | `tc netem delay 100ms` |
| `loss_5pct` | `tc netem loss 5%` |
| `bw_cap_1mbps` | `tc tbf rate 1mbit` |
| `mixed` | 50ms latency + 2% loss |

Each scenario runs 3 repetitions. Results are written to `results/results.csv`. `analyze` prints:

```
Scenario             │ Static      Adaptive    Δ time  │ Failures(S/A) │ Switches(S/A)
latency_100ms        │ 1.234s      0.891s      -27.8%  │ 3 / 1         │ 2 / 0
loss_5pct            │ 2.341s      1.567s      -33.1%  │ 12 / 4        │ 4 / 2
```

The adaptive scheduler wins under `latency_100ms` and `loss_5pct` because:

- **RTT**: the scorer penalises `rttNorm = 1 / (1 + RTT / 50ms)` — a 200ms peer scores 0.20 on the RTT term vs 0.50 for a 50ms peer.
- **Loss**: `lossScore = 1 − lossRate` — a 10% loss peer scores 0.90 vs 1.0 for a clean peer.
- **Starvation prevention**: when the best peer is busy, any worker falls back to claiming the piece (second-pass fallback in `pickPiece`).

Under `baseline`, static ≈ adaptive — overhead of the scorer is 18 ns/op, negligible.

---

## Scoring function

```
score(peer) =
    0.35 × min(throughput / 10 Mbps, 1.0)
  + 0.30 × 1 / (1 + RTT_ms / 50)
  + 0.25 × (1 − lossRate)
  + 0.05 × stability
  − 0.05 × (activeReqs / 4)
```

Weights are tunable via `scheduler.Weights`. Measurements use EWMA: α=0.5 for RTT (fast adapt), α=0.3 for throughput, α=0.2 for loss (needs stability).

---

## Testing

```bash
make test        # unit + integration + scenario tests
make test-race   # same with race detector (all clean)
make bench       # scheduler benchmarks
```

Key benchmark numbers (Intel Xeon @ 2.1 GHz):

| Benchmark | Time | Allocs |
|---|---|---|
| `BenchmarkScore` | 18 ns/op | 0 |
| `BenchmarkBestPeer_5Peers` | 682 ns/op | 0 |
| `BenchmarkBestPeer_50Peers` | 6.9 µs/op | 0 |
| `BenchmarkBestPeer_100Peers` | 13.5 µs/op | 0 |
| `BenchmarkRarestMissing_4096Pieces` | 49 µs/op | 19 allocs |
| `BenchmarkRarestMissing_256Pieces` | 9.6 µs/op | 12 allocs |

The hot path (Score + BestPeer + ShouldYield) has zero allocations. The sort in `RarestMissing` allocates once per call — this is called at most once per workerPoll interval (30ms), so 19 allocations every 30ms is inconsequential.

---

## Phase roadmap

| Phase | Status | What |
|---|---|---|
| 0 | ✅ | Protocol, piece chunker/assembler, manifest, store |
| 1 | ✅ | Tracker, seeder, leecher, basic TCP transfer |
| 2 | ✅ | Parallel download, rarest-first, peer churn, resume |
| 3 | ✅ | Monitor (RTT/throughput/loss EWMA), scorer, adaptive selection |
| 4 | ✅ | Experiment binary, analysis tool, benchmarks |
| v2 | planned | DHT tracker (Kademlia), erasure coding, QUIC transport |
| v3 | planned | NAT traversal, predictive scheduling, incentive mechanism |

---

## CN concepts demonstrated

- **TCP socket programming** — raw `net.Conn`, custom length-prefixed framing, concurrent reader/writer goroutines
- **Congestion-aware routing** — RTT and throughput measured per-peer, used for scheduling decisions
- **Packet loss estimation** — sliding window of probe outcomes, EWMA smoothing
- **Peer churn resilience** — claim/release mechanism; failed worker releases its in-flight piece, surviving workers pick it up
- **Network emulation** — `tc netem` for controlled latency/loss injection
- **Distributed coordination** — central tracker + decentralised piece transfer; rarest-first to prevent piece unavailability
