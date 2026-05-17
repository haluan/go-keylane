# go-keylane

A Go library for routing jobs by key into deterministic execution lanes, improving fairness, isolation, and tail-latency control in high-throughput backend services.

> Status: pre-v0.1 experimental. Public APIs may change.

---

## Installation

To start using `go-keylane` in your project, install the module via Go CLI:

```bash
go get github.com/haluan/go-keylane
```

---

## Core Concepts

- **Key**: A business identity (e.g., tenant ID, customer ID, order ID) used to route execution deterministically.
- **Lane**: A classification grouping similar jobs (e.g., payment, audit, webhook) with distinct processing priorities.
- **Shard**: A concurrency isolation bucket derived from hashing the job's Key, preventing noisy neighbors from blocking quiet peers.
- **Quota**: The maximum number of jobs from a specific Lane that a worker will process in a single pass over a Shard.
- **Worker**: A goroutine thread running in the scheduler loop that pops ready shards and processes ready lane queues.

---

## Fire-and-Forget Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/haluan/go-keylane"
)

func main() {
	cfg := keylane.Config{
		ShardCount:       8,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas: map[keylane.Lane]int{
			"payment": 3,
			"webhook": 1,
		},
	}

	q, err := keylane.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start workers
	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}

	// Submit a fire-and-forget job
	err = q.Submit(ctx, keylane.Job{
		Key:  "tenant-123",
		Lane: "payment",
		Run: func(ctx context.Context) error {
			fmt.Println("Processing payment asynchronously!")
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Stop gracefully when done
	_ = q.Stop(ctx, keylane.WithDrain(true))
}
```

---

## SubmitValue & Await Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/haluan/go-keylane"
)

func main() {
	q, _ := keylane.New(keylane.Config{
		ShardCount:       4,
		WorkerCount:      2,
		QueueSizePerLane: 100,
		LaneQuotas:       map[keylane.Lane]int{"payment": 1},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = q.Start(ctx)

	// Submit a job expecting a result
	future, err := keylane.SubmitValue(ctx, q, keylane.ValueJob[string]{
		Key:  "user-456",
		Lane: "payment",
		Run: func(ctx context.Context) (string, error) {
			return "processed_invoice_99", nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Await the value with a timeout context
	awaitCtx, awaitCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer awaitCancel()

	result, err := future.Await(awaitCtx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Received result: %s\n", result)
}
```

---

## Configuration

The `Config` struct controls how shard isolation, worker pools, and lane-level processing quotas are scoped globally:

| Option | Type | Description |
| :--- | :--- | :--- |
| `ShardCount` | `int` | Number of distinct concurrency buckets. Keys are hashed into these shards. |
| `WorkerCount` | `int` | Number of parallel worker goroutines popping active shards. |
| `QueueSizePerLane` | `int` | Bounded queue capacity allocated per lane inside a single shard. |
| `LaneQuotas` | `map[Lane]int` | The relative execution limits processed per lane in one worker pass. |
| `Observability` | `ObservabilityConfig` | Configuration settings controlling queue wait time, stats, or slow job hooks. |

---

## What go-keylane is

`go-keylane` is an in-process execution control library for Go services designed to regulate concurrent request traffic. It provides:
- **Noisy key/tenant isolation** preventing hot users from hogging resources.
- **Fair resource distribution** among different job priority classes sharing a worker pool.
- **Bounded memory allocation patterns** through map-free ring buffers and pooled collections.

---

## What go-keylane is NOT

`go-keylane` is **not**:
- A replacement for the Go scheduler or the OS scheduler.
- A distributed queue or message broker (like RabbitMQ or Redis).
- A persistent job system (it operates entirely in-memory and state is lost on process restart).

> [!IMPORTANT]
> **go-keylane does not avoid Go GC pauses.**
> go-keylane helps reduce GC pressure caused by uncontrolled concurrency, goroutine explosion, unbounded queues, and allocation bursts.

---

## Await Deadlock Risk Warning

> [!CAUTION]
> **Never call `Await` inside a worker `Run` function on the same queue.**
>
> Doing so creates a high risk of **resource exhaustion deadlocks**. If your `WorkerCount` is small (e.g. 1) and that worker picks up a job that blocks on `Await` for another job submitted to the *same* queue, the worker will wait forever, deadlock the scheduler, and starve all other tasks.
>
> **Safe Alternatives:**
> - Submit separate fire-and-forget jobs and coordinate results in the outer caller context using standard tools like `sync.WaitGroup`.
> - Use independent `Queue` instances if jobs must have caller-dependent execution pipelines.

---

## Documentation

For deep-dive architecture, guides, and benchmarking notes:

- [Quickstart Guide](docs/quickstart.md)
- [Architecture & Design Details](docs/design.md)
- [Operational & Production Guidance](docs/production-guidance.md)
- [Debugging Guide](docs/debugging.md)
- [Glossary of Terms](docs/glossary.md)
- [Performance & Benchmarking](docs/benchmarks.md)

---

## License

`go-keylane` is distributed under the GNU General Public License v3. See the [LICENSE](LICENSE) file for details.
