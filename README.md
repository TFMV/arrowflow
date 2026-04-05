# ArrowFlow

ArrowFlow is a Go evaluation harness for a streaming Protobuf -> Apache Arrow pipeline built around `bufarrowlib`, NATS JetStream, and an optional HyperType JIT decode path.

The repo now runs a real replicated STREAM benchmark path:
- 3-node JetStream cluster
- file-backed streams
- `Replicas=3`
- explicit consumer ack after Arrow batch flush
- worker-local transcoder isolation with a shared, concurrency-safe HyperType
- corpus-first timed runs in `direct`, `stream`, and `stress` modes

## Quick Start

```bash
make build

docker compose up -d

export NATS_URL="nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"
export STREAM_REPLICAS=3
export STREAM_STORAGE=file

./scripts/run-all-experiments.sh
```

The suite restarts the cluster, runs the full STREAM matrix, and writes:
- `results/all-experiments/results.csv`
- `results/all-experiments/*.txt`

## Cluster Layout

`docker-compose.yaml` starts:
- `nats-1` on `4222`, monitor `8222`
- `nats-2` on `4223`, monitor `8223`
- `nats-3` on `4224`, monitor `8224`
- `nats-box` for CLI inspection inside the Docker network

`nats.conf` enables JetStream, file storage under `/data/jetstream`, HTTP monitoring, and cluster routing on `6222`.

## Commands

```bash
./bin/arrowflow experiment --mode stream --rate 50000 --duration 15s --workers 8 --batch-size 1000 --hyper=true --denorm=true

./bin/arrowflow experiment --mode stream --rate 0 --duration 10s --workers 16 --batch-size 500 --hyper=true --denorm=true

./bin/arrowflow experiment --mode direct --rate 10000 --duration 15s --workers 4 --batch-size 1000 --hyper=true

./bin/arrowflow benchmark --messages 100000 --size 1024 --workers 8 --batch-size 2000 --hyper=true --denorm=false

./bin/arrowflow chaos --rate 25000 --duration 20s --mode burst --workers 8 --hyper=true
```

Important flags:
- `--mode`: `stream`, `direct`, `stress`
- `--size-dist`: `small`, `medium`, `large`, `heavy-tail`
- `--denorm`: flattened Arrow output with repeated-field fanout
- `--batch-size`: Arrow flush threshold per worker
- `--hyper`: enable HyperType JIT parsing

## Current Result Snapshot

These numbers come from the latest `scripts/run-all-experiments.sh` run on April 5, 2026, plus direct replay sweeps using the same corpus-first harness.

- All timed modes now prebuild a bounded raw-wire corpus before the clock starts. `stream` mode replays that corpus through JetStream instead of generating payloads on the publish path.
- The direct `bufarrowlib` replay path is comfortably above `100k msg/s`. The best nested replay run on the fixed `100000`-message, `1024`-byte corpus reached `182999.32 msg/s` and `476.36 MB/s` with `8` workers and `batch=2000`.
- The direct denormalized replay path also crossed the line: `113801.76 msg/s` and `296.62 MB/s` on the same corpus with `batch=2000`.
- A rate-limited heavy-tail direct run still sustained `50000.70 msg/s` for `10s`, at `445.96 MB/s`.
- The replicated STREAM path remains the visible ceiling. The best saturation point in the latest suite was `5889.87 msg/s` at `200000 msg/s` offered load.
- HyperType still reduces consumer-side latency, but the broker path dominates end-to-end throughput in STREAM mode. The latency speedups were `1.43x` on `small`, `1.61x` on `medium`, `1.52x` on `large`, and `1.53x` on `heavy-tail`.
- The best heavy-tail STREAM batch size in this run was `100`: `5419.90 msg/s` with `45.2 us` mean consume latency. `batch=5000` and `batch=10000` pushed heap to `1.67 GB` and `2.43 GB`.
- Nested output won the replicated STREAM throughput comparison in this run: `5189.99 msg/s` vs `4803.75 msg/s`. Denorm still kept lower consume latency (`47.9 us` vs `52.2 us`) while averaging `72.36x` row fanout.
- Worker scaling peaked before `16` workers: `2 workers` reached `3047.43 msg/s`, `8 workers` reached `4806.68 msg/s`, and `16 workers` slipped to `4590.07 msg/s`.
- Chaos mode consumed `89893` messages over `20s`, with `48.2 us` mean consume latency, `1.74 ms` mean produce latency, peak consumer lag `298`, and peak buffer depth `7821`.

## What Was Fixed

The harness and pipeline now avoid the main integrity failures from the earlier audit:
- JetStream messages are acked only after successful Arrow batch flush.
- Consumer transcoders are released exactly once.
- Subcommand flag parsing is isolated and correct.
- All timed modes now use prebuilt raw-message corpora, explicit HyperType warmup/recompile, flush batches, and terminate cleanly.
- STREAM runs use real repeated-field denorm paths.
- STREAM runs use all requested producer workers, support `--rate 0` max-throughput mode, and consume via parallel JetStream pull workers instead of a single callback funnel.
- Replicated JetStream is explicit instead of silently falling back to plain publish.
- Observability overhead was moved out of the per-message hot path so replay benchmarks measure the pipeline instead of Prometheus bookkeeping.
- Per-run result extraction is based on final command output, not fragile intermediate telemetry files.

## Notes

- This is still a single-host benchmark harness. The broker path is truly replicated JetStream, but all three nodes run on one machine, so network and disk findings should be interpreted as single-host cluster behavior.
- `bufarrowlib` `Transcoder` instances are worker-local and must not be shared across goroutines. `HyperType` is safe to share across cloned transcoders.
- Arrow record batches must always be released.
