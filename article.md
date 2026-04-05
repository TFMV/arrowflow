# I Finally Got a Clean Protobuf → Arrow Pipeline—Then the Bottleneck Moved

A friend of mine built a library that immediately made me rethink a bunch of ingestion code I had accepted as inevitable.

It is called `bufarrowlib`, and the idea is almost suspiciously clean:

take raw Protobuf wire bytes, point the library at a descriptor, and write Apache Arrow batches directly without the usual detour through generated Go structs and hand-written `RecordBuilder` glue.

If you have built enough pipelines, that pitch hits right in the scar tissue.

Because the normal pipeline usually looks like this:

1. receive bytes from Kafka or NATS
2. decode into generated structs
3. walk those structs again to fill Arrow builders
4. redo the mapping every time the schema gets interesting

That is a lot of CPU, a lot of allocation, and a lot of code nobody actually wants to own.

`bufarrowlib` goes after that entire layer.

## Why I Built A Whole Harness Around It

The reason I got excited is that I work on systems where the ingestion path matters as much as the analytics path. If the first hop is expensive, every downstream optimization is fighting uphill.

So when my friend showed me a library that could go from raw wire bytes to Arrow memory directly, I did not want a toy benchmark. I wanted to know:

- does this hold up under real broker pressure?
- what does denormalization actually cost?
- when does HyperType help enough to matter?
- where does the bottleneck move once decoding gets cheaper?

That is why I built ArrowFlow: not as a microbenchmark, but as a way to push the whole path until the real bottlenecks showed up.

## The Pipeline I Wanted To Measure

ArrowFlow is a Go harness around four moving pieces:

- a synthetic Protobuf event generator
- a NATS JetStream transport layer
- a `bufarrowlib` consumer with optional HyperType JIT parsing
- an Arrow batch flush path with denormalized and nested modes

At a high level, the system looks like this:

```mermaid
flowchart LR
    A["Producer<br/>raw protobuf bytes"] --> B["NATS JetStream<br/>3-node replicated stream"]
    B --> C["Worker-local bufarrowlib transcoder"]
    C --> D["Arrow RecordBatch flush"]
    D --> E["Ack after successful batch output"]
```

That last step matters more than it looks. In a real streaming system, the right place to ack is after the Arrow write path succeeds, not when the message merely lands in an in-process queue.

## What I Like About `bufarrowlib`

The library is interesting to me because it changes where complexity lives.

Instead of writing procedural mapping code, you declare the shape you want.

A simplified setup from ArrowFlow looks like this:

```go
fd, err := ba.CompileProtoToFileDescriptor(protoFile, []string{protoDir})
if err != nil {
	return nil, err
}

md, err := ba.GetMessageDescriptorByName(fd, "Event")
if err != nil {
	return nil, err
}

ht := ba.NewHyperType(md, ba.WithAutoRecompile(0, 1.0))

opts := []ba.Option{
	ba.WithHyperType(ht),
	ba.WithDenormalizerPlan(
		pbpath.PlanPath("schema_version"),
		pbpath.PlanPath("event_timestamp"),
		pbpath.PlanPath("user.user_id"),
		pbpath.PlanPath("session.session_id"),
		pbpath.PlanPath("tracing.trace_id"),
		pbpath.PlanPath("payload.event_type"),
		pbpath.PlanPath("metrics[*].name"),
		pbpath.PlanPath("tags[*].key"),
	),
}

tc, err := ba.New(md, memory.DefaultAllocator, opts...)
```

That denorm plan is the whole point. The interesting thing is not just that the library understands Protobuf. It is that it lets you describe the Arrow shape you want without writing the usual pile of structural traversal code by hand.

The repeated fields are explicit:

- `metrics[*].name`
- `tags[*].key`

That means the fanout is not hypothetical. The transcoder is really flattening repeated structures into Arrow rows.

## The Benchmarking Change That Finally Made The Numbers Honest

The biggest harness improvement was embarrassingly simple:

stop generating payloads while the timer is running

Every timed mode in ArrowFlow now starts by preparing a bounded corpus of raw Protobuf wire bytes. Then the benchmark replays that corpus.

- `direct` replays it in process
- `stress` replays the same corpus across multiple rate targets
- `stream` replays the exact same raw messages through JetStream

That means the timed path is now measuring replay, transport, parsing, Arrow append, and batch flush.

Not random string generation.
Not `crypto/rand`.
Not a synthetic producer fighting the consumer for CPU while pretending to be part of the ingestion library.

The warmup path looks like this:

```go
corpusPlan, err := producer.PrepareRawCorpus(
	producer.WireConfig{SizeDist: "heavy-tail", SchemaVersion: "1.0.0"},
	producer.CorpusOptions{TargetMessages: 100000, PilotMessages: 1024},
)
if err != nil {
	return err
}

corpus := corpusPlan.Messages
if err := warmTranscoder(base, ht, denorm, corpus); err != nil {
	return err
}
```

And in stream mode, the timed producer just replays those same raw payloads into NATS:

```go
raw := nextRaw(shard, &cursor)
if err := pub.Publish(ctx, topic, &stream.Msg{Payload: raw}); err != nil {
	return err
}
```

## The Concurrency Rule You Really Cannot Ignore

One detail my friend pushed me on, correctly, is worth being precise about:

- `HyperType` is safe to share
- `Transcoder` is not

That means the right pattern is not “one HyperType per worker.” It is “one shared HyperType, cloned transcoders per worker.”

The ArrowFlow worker model is deliberately boring:

```go
transcoders := []*ba.Transcoder{base}
for i := 1; i < workers; i++ {
	clone, err := base.Clone(memory.NewGoAllocator())
	if err != nil {
		return err
	}
	transcoders = append(transcoders, clone)
}
```

Under the hood, those clones keep sharing the same `*HyperType`, which is exactly what you want. You pay the compile cost once, warm it up once, then keep worker-local append state.

That looks like a small detail, but it is the difference between a serious pipeline and a benchmark that lies to you.

Each worker gets:

- its own transcoder clone
- access to the same HyperType
- its own append path
- its own batch flush cadence

That keeps the library in the space it was designed for, and it keeps the benchmark from smuggling shared-state problems into the results.

## The Systems Detail That Matters Most

Once I had a reliable broker path in place, the core ingestion loop became this:

```go
for item := range wc.inputChan {
	raw := item.msg.Payload

	if err := tc.AppendDenormRaw(raw); err != nil {
		_ = item.msg.Term()
		continue
	}

	pending = append(pending, item.msg)
	if len(pending) >= wc.batchSize {
		flushBatch(tc, pending)
		pending = pending[:0]
	}
}
```

And the important part of `flushBatch` is not the batch creation itself. It is the settlement rule:

```go
rec := tc.NewDenormalizerRecordBatch()
defer rec.Release()

for _, msg := range pending {
	if err := msg.Ack(); err != nil {
		log.Printf("batch ack failed: %v", err)
	}
}
```

This is one of those systems details that sounds boring until it is wrong.

When it is wrong, you get benchmarks that look fast and pipelines that lose data.

## The Broker Setup I Used

For the latest run I used a 3-node JetStream cluster, file-backed, with `Replicas=3`.

That matters because I was not interested in measuring a local happy-path publish loop. I wanted to know what happens once coordination is part of the hot path.

The topology is simple:

```mermaid
flowchart LR
    P["ArrowFlow producer"] --> N1["nats-1"]
    P --> N2["nats-2"]
    P --> N3["nats-3"]

    subgraph JetStream Cluster
        N1 <--> N2
        N2 <--> N3
        N1 <--> N3
    end

    N1 --> C["ArrowFlow consumer group"]
    N2 --> C
    N3 --> C
```

All three nodes were on one machine, so this is still single-host cluster behavior, not a true multi-host distributed benchmark. But it is enough to expose the coordination cost, which is what I cared about most for this round.

## What The Numbers Actually Say

After fixing several harness issues that were masking configuration differences, I reran the full STREAM suite and rebuilt the timed modes around corpus replay.

The data lives in `results/all-experiments/results.csv`, but the most interesting parts are easy to summarize.

### The direct path is the first thing I wanted to sanity-check

Before talking about the broker, I wanted to know whether I was using `bufarrowlib` correctly.

The first direct replay result that made me relax was the nested path on a fixed corpus:

- `100000` prebuilt messages
- fixed `1024`-byte target size
- `8` workers
- `batch=2000`
- HyperType enabled

That landed at:

- `182999.32 msg/s`
- `476.36 MB/s`

And the denormalized version of the same replay still cleared the line comfortably:

- `113801.76 msg/s`
- `296.62 MB/s`

Then I ran the heavier rate-limited replay with heavy-tail payloads:

- `50000.70 msg/s` for `10s`
- `445.96 MB/s`
- `89.7 us` mean consume latency
- `8.9 us` mean batch-output latency

That was the first reassuring number in the whole project.

It told me the low STREAM numbers were not “`bufarrowlib` is slow.”

They were “the bottleneck moved.”

### HyperType is not a rounding error, but it is not the system ceiling either

On the replicated STREAM path, mean consume latency dropped by:

- `1.43x` on small messages: `20.6 us` -> `14.4 us`
- `1.61x` on medium messages: `34.7 us` -> `21.6 us`
- `1.52x` on large messages: `160.3 us` -> `105.5 us`
- `1.53x` on heavy-tail messages: `73.1 us` -> `47.8 us`

That is strong enough that I would enable it by default unless I had a very specific reason not to.

The interesting wrinkle is that throughput did not improve with it in the crossover STREAM runs. HyperType made the consumer cheaper, but the end-to-end rate stayed flat or slipped slightly because the broker path was already the thing deciding the pace.

### The best STREAM batch size was actually `100`

At `50000 msg/s` offered load with heavy-tail payloads:

- `batch=100` reached `5419.90 msg/s`, `45.2 us` mean consume latency, `473.48 MB` heap
- `batch=500` reached `4936.59 msg/s`, `47.0 us`, `689.71 MB` heap
- `batch=1000` reached `4750.48 msg/s`, `49.2 us`, `856.66 MB` heap
- `batch=5000` reached `3761.00 msg/s`, `69.5 us`, `1666.72 MB` heap
- `batch=10000` reached `3416.62 msg/s`, `76.3 us`, `2425.05 MB` heap

This is exactly the trade you would expect in a real pipeline: after a point, “bigger batch” mostly means “more queued memory.”

### Denormalization was the real surprise

The surprise changed once the benchmark got cleaner.

At `50000 msg/s` offered load:

- nested mode: `5189.99 msg/s`, `52.2 us` mean consume latency
- denorm mode: `4803.75 msg/s`, `47.9 us` mean consume latency

And this was not fake fanout. The heavy-tail denorm path averaged:

- `72.36x` denorm fanout
- `8` Arrow columns in the selected plan
- `69,314` average rows per emitted batch in the denorm run

The updated lesson is more nuanced than the earlier run.

Denorm was still cheaper inside the consumer. Mean consume latency was lower even with all that fanout.

But end-to-end throughput still favored nested output on the replicated STREAM path.

That tells me the extra cost moved somewhere outside the local append path. Once the hot path becomes coordination-bound, “faster inside the transcoder” and “higher end-to-end throughput” stop being the same statement.

That result is specific to this event shape and this denorm plan, but it is still impressive. My friend’s library is doing real structural work here, not just flattening a few scalars.

### The broker path became the long pole

The saturation sweep peaked at:

- `5889.87 msg/s` observed throughput at `200000 msg/s` offered load
- `43.4 us` mean consume latency
- `1.36 ms` mean produce latency

The point is not that `200000` is some magic offered rate.

The point is that once the direct replay path is comfortably above `100k msg/s`, a replicated single-host JetStream cluster topping out around `5k-6k msg/s` becomes impossible to miss.

That last number is the tell.

Once the parser and Arrow append path get cheap enough, the bottleneck moves out toward:

- replicated broker admission
- producer-side backpressure
- batch cadence
- queue depth

That is exactly the shift I wanted the harness to expose. Once the local decode path gets good enough, the bottleneck stops hiding.

## The Moment The Bottleneck Moved

At some point in this run, the results stopped being about parsing.

The numbers flattened, but not because the system was idle.

They flattened because the hot path had moved.

Not inside the process.
Not inside the library.

Out into the broker.

Once Protobuf decoding and Arrow writes got cheap enough, the system stopped being CPU-bound and became coordination-bound.

That is the line I was trying to find.

## Where The System Actually Starts To Bend

One thing I like about this run is that the failure surfaces are visible.

At high saturation and in chaos mode, the system did not primarily collapse into broker lag. It tended to accumulate in-process buffer depth first.

The chaos run looked like this:

- `89893` consumed messages in `20s`
- `1.74 ms` mean produce latency
- `48.2 us` mean consume latency
- `238.38 MB` heap allocation
- `7821` peak buffer depth
- `298` peak consumer lag

That tells me the first pressure surface in this environment is consumer-side buffering and batch formation, not the broker falling irrecoverably behind.

## The Worker Story Was Less Exciting Than People Usually Hope

I also swept worker counts, because everybody always asks whether more goroutines fix the problem.

At `50000 msg/s` offered load:

- `2 workers`: `3047.43 msg/s`, `34.9 us` mean consume latency
- `4 workers`: `3997.27 msg/s`, `45.5 us`
- `8 workers`: `4806.68 msg/s`, `48.9 us`
- `16 workers`: `4590.07 msg/s`, `60.8 us`

So no, this was still not a case where “just add workers” solved everything. But once I fixed the single-callback fetch path, workers finally started to matter in the place they should have mattered: broker pull concurrency. They just stopped helping before `16`.

## Why I Think `bufarrowlib` Matters

What my friend built is not just a faster parser.

It is a library that removes an entire category of ingestion code:

- less generated-struct churn
- less handwritten field mapping
- less object materialization
- fewer copies before Arrow

That is the architectural win.

The performance win is real, but the bigger idea is that the intermediate Go struct was never a law of nature. It was just the thing most of us tolerated because the alternatives were worse.

`bufarrowlib` is the first Go library I have used in this space that made that assumption feel unnecessary.

## The Part I Keep Coming Back To

The story I wanted to tell after building ArrowFlow is not “my friend made a magic library.”

It is more interesting than that.

The story is:

- yes, direct Protobuf -> Arrow is a real systems win
- yes, HyperType materially improves the hot path
- yes, real denormalization can still be fast enough to cross `100k msg/s` on replay
- and once those pieces get efficient enough, the bottleneck moves exactly where a distributed systems engineer would expect it to move

from parsing to coordination

from local CPU to replicated admission

from clever decode logic to queueing, batching, and backpressure

That is the result I trust most.

And as someone who has spent too much time maintaining the old shape of these pipelines, I am genuinely happy my friend built the thing that made me question it.
