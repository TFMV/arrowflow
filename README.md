# ArrowFlow

ArrowFlow is a Go evaluation harness for a streaming Protobuf -> Apache Arrow pipeline built around `bufarrowlib`, NATS JetStream, and an optional HyperType JIT decode path.

The repo now runs a real replicated STREAM benchmark path:
- 3-node JetStream cluster
- file-backed streams
- `Replicas=3`
- explicit consumer ack after Arrow batch flush
- per-worker transcoder isolation

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

./bin/arrowflow experiment --mode direct --rate 10000 --duration 15s --workers 4 --batch-size 1000 --hyper=true

./bin/arrowflow chaos --rate 25000 --duration 20s --mode burst --workers 8 --hyper=true
```

Important flags:
- `--mode`: `stream`, `direct`, `stress`
- `--size-dist`: `small`, `medium`, `large`, `heavy-tail`
- `--denorm`: flattened Arrow output with repeated-field fanout
- `--batch-size`: Arrow flush threshold per worker
- `--hyper`: enable HyperType JIT parsing

## Current Result Snapshot

These numbers come from the latest `scripts/run-all-experiments.sh` run on April 4, 2026 against the replicated STREAM path.

- HyperType reduced mean consume latency by `2.95x` on `small`, `1.95x` on `medium`, `1.52x` on `large`, and `2.02x` on `heavy-tail`.
- Best batch-size balance in this environment was `1000`: `2402.62 msg/s` at `36.5 us` mean consume latency.
- `batch=10000` did not improve throughput and drove heap usage to `1477.80 MB` with `32986` messages buffered.
- Denormalized output outperformed nested output in this schema: `2932.68 msg/s` vs `1998.80 msg/s`, with mean consume latency `32.3 us` vs `52.8 us`.
- Denorm fanout was real, not synthetic: about `72x` on heavy-tail workloads and about `110x` on large workloads.
- Peak observed STREAM throughput was `5404.24 msg/s` at `200000 msg/s` offered load.
- Worker scaling was not linear. In this single-host cluster run, `2 workers` and `16 workers` were similar on throughput, while `8 workers` regressed on latency and throughput.
- Chaos mode consumed `28892` messages over `20s`, with peak buffer depth `7817` and no sustained consumer lag.

## What Was Fixed

The harness and pipeline now avoid the main integrity failures from the earlier audit:
- JetStream messages are acked only after successful Arrow batch flush.
- Consumer transcoders are released exactly once.
- Subcommand flag parsing is isolated and correct.
- Direct benchmarks flush batches and terminate cleanly.
- STREAM runs use real repeated-field denorm paths.
- Replicated JetStream is explicit instead of silently falling back to plain publish.
- Per-run result extraction is based on final command output, not fragile intermediate telemetry files.

## Notes

- This is still a single-host benchmark harness. The broker path is truly replicated JetStream, but all three nodes run on one machine, so network and disk findings should be interpreted as single-host cluster behavior.
- `bufarrowlib` transcoders and HyperType contexts are still per-worker resources. Do not share them across goroutines.
- Arrow record batches must always be released.
