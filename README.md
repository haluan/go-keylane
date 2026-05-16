# go-keylane
A Go library for routing jobs by key into deterministic execution lanes, improving fairness, isolation, and tail-latency control in backend services.

It helps Go services execute asynchronous jobs more fairly by routing work through:

- **Key**: business identity such as tenant, customer, account, order, or user
- **Lane**: job class such as payment, audit, webhook, email, or enrichment
- **Shard**: concurrency isolation bucket derived from the key
- **Quota**: per-lane execution allowance
- **Worker**: goroutine that processes ready shards

## Mental Model

```mermaid
flowchart TD
    A["Request / Job"] --> B["Key"]
    B --> C["hash(key) % shard_count"]
    C --> D["Shard"]

    D --> E["payment lane<br/>quota = 3"]
    D --> F["audit lane<br/>quota = 1"]
    D --> G["webhook lane<br/>quota = 1"]

    E --> H["Worker"]
    F --> H
    G --> H
```

## What go-keylane is

`go-keylane` is an in-process execution control library for Go services.

It is designed to help with:

* noisy tenant/key isolation
* fairer execution between job classes
* bounded queueing
* controlled worker goroutine count
* lower allocation pressure through bounded internal structures

## What go-keylane is not

`go-keylane` is not:

* a replacement for the Go scheduler
* a replacement for the OS scheduler
* a distributed queue
* a Redis/Postgres-backed job system
* a guarantee that code will always run faster
* a way to avoid Go GC pauses

`go-keylane` may help reduce GC pressure caused by uncontrolled concurrency, goroutine explosion, and allocation bursts, but it does not eliminate garbage collection.

## Example Use Case

A payment service may want to process work by customer:

* `payment` lane gets higher quota
* `audit` lane gets lower quota
* `webhook` lane gets bounded background execution
* noisy customer keys should not dominate all workers
