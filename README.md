# ArrowFlow

ArrowFlow is a Go evaluation harness for a streaming Protobuf -> Apache Arrow pipeline built around `bufarrowlib`, NATS JetStream, and an optional HyperType JIT decode path.

The repo now runs a real replicated STREAM benchmark path:
- 3-node JetStream cluster
- file-backed streams
- `Replicas=3`
- explicit consumer ack after Arrow batch flush
- worker-local transcoder isolation with a shared, concurrency-safe HyperType

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

./bin/arrowflow benchmark --messages 100000 --size 1024 --workers 8 --batch-size 1000 --hyper=true --denorm=true

./bin/arrowflow chaos --rate 25000 --duration 20s --mode burst --workers 8 --hyper=true
```

Important flags:
- `--mode`: `stream`, `direct`, `stress`
- `--size-dist`: `small`, `medium`, `large`, `heavy-tail`
- `--denorm`: flattened Arrow output with repeated-field fanout
- `--batch-size`: Arrow flush threshold per worker
- `--hyper`: enable HyperType JIT parsing

## Current Result Snapshot

These numbers come from the latest `scripts/run-all-experiments.sh` run on April 5, 2026, plus one direct corpus benchmark pass with the same corrected harness.

- The direct `bufarrowlib` path is not the bottleneck in this repo anymore. `./bin/arrowflow benchmark --messages 100000 --size 1024 --workers 8 --batch-size 1000 --hyper=true --denorm=true` reached `91417.93 msg/s` and `237.73 MB/s`.
- A heavy-tail direct run sustained `49999.20 msg/s` for `10s`, with `35.8 us` mean consume latency and `6.4 us` mean batch-output latency.
- The replicated STREAM path plateaus much lower, which is the point of the harness: the best saturation result was `4680.39 msg/s` at `150000 msg/s` offered load, and `200000 msg/s` was essentially flat at `4661.59 msg/s`.
- HyperType still materially lowers consume latency: `2.71x` on `small`, `2.49x` on `medium`, `1.39x` on `large`, and `1.90x` on `heavy-tail`.
- Throughput gains from HyperType are no longer monotonic in STREAM mode. Once the run is broker-bound, lower consume latency does not always translate into higher end-to-end throughput.
- The best heavy-tail STREAM batch size in this run was `500`: `4838.98 msg/s` with `35.8 us` mean consume latency. `batch=5000` and `batch=10000` pushed heap above `1.9 GB` and `2.1 GB`.
- Denormalized output still wins on this schema despite real fanout: `4639.97 msg/s` vs `4492.68 msg/s`, with `36.2 us` vs `46.1 us` mean consume latency and `71.79x` average row fanout.
- Worker scaling now shows the broker fetch path more clearly: `2 workers` reached `2540.71 msg/s`, `8 workers` reached `4247.79 msg/s`, and `16 workers` reached `4692.71 msg/s`.
- Chaos mode consumed `87418` messages over `20s`, with `34.6 us` mean consume latency, `1.79 ms` mean produce latency, peak consumer lag `108`, and peak buffer depth `7875`.

## What Was Fixed

The harness and pipeline now avoid the main integrity failures from the earlier audit:
- JetStream messages are acked only after successful Arrow batch flush.
- Consumer transcoders are released exactly once.
- Subcommand flag parsing is isolated and correct.
- Direct benchmarks now use prebuilt raw-message corpora, explicit HyperType warmup/recompile, flush batches, and terminate cleanly.
- STREAM runs use real repeated-field denorm paths.
- STREAM runs use all requested producer workers, support `--rate 0` max-throughput mode, and consume via parallel JetStream pull workers instead of a single callback funnel.
- Replicated JetStream is explicit instead of silently falling back to plain publish.
- Per-run result extraction is based on final command output, not fragile intermediate telemetry files.

## Notes

- This is still a single-host benchmark harness. The broker path is truly replicated JetStream, but all three nodes run on one machine, so network and disk findings should be interpreted as single-host cluster behavior.
- `bufarrowlib` `Transcoder` instances are worker-local and must not be shared across goroutines. `HyperType` is safe to share across cloned transcoders.
- Arrow record batches must always be released.
