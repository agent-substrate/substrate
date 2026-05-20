# Aerospike Storage Backend for Agent Substrate

This package provides a high-performance, durable persistence layer implementation for Agent Substrate backed by **Aerospike**. It satisfies the `store.Interface` contract, allowing Substrate to manage high-frequency Actor and Worker states with sub-millisecond latency and robust data durability.

---

## 1. Architecture & Data Model

Aerospike stores records in **Namespaces** (logical databases) and **Sets** (logical tables). In this implementation:

* **Namespace**: Default is `test` (fully configurable at startup).
* **Sets**:
  * `actors`: Stores actor states under key format `<actor-id>`.
  * `workers`: Stores physical worker pod states under key format `<namespace>:<pool-name>:<pod-name>`.
  * `locks`: Stores active distributed mutex records under key `<lock-key>`.

### Mapping Strategy
* **Bins (Fields)**:
  * `data`: Stores the entire serialized Protobuf state as a JSON string (`protojson.Marshal`).
  * `val`: Stores the unique lock owner token inside the `locks` set.
* **Generation-to-Version Concurrency**:
  We map Aerospike's internal server-side record `Generation` number directly to the Substrate record `Version` field. Mutative updates leverage Aerospike's native optimistic locking (`EXPECT_GEN_EQUAL`), preventing concurrent overwrite conflicts atomically in-memory.

---

## 2. Local Development Setup (Docker)

To run Aerospike locally in Docker, you must increase the maximum file descriptor limit. By default, the Aerospike server requires a soft limit of 15,000 descriptors, whereas Docker containers default to 1,024, causing it to crash on startup.

Start the container with the custom ulimit flag:

```bash
# 1. Spin up the Aerospike container
docker run -d \
  --name aerospike \
  --ulimit nofile=20000:20000 \
  -p 3000-3002:3000-3002 \
  -p 3003:3003 \
  aerospike/aerospike-server
```

Verify it is running:
```bash
docker logs aerospike
```

---

## 3. Testing

This package comes with a comprehensive unit and integration test suite in `ateaerospike_test.go`.

To prevent breaking default local developer environments, the test suite contains **automated connection detection**. If no Aerospike database is listening on `localhost:3000`, the suite will gracefully skip the integration tests instead of failing.

Run the tests:
```bash
go test -v ./cmd/servers/ateapi/store/ateaerospike/...
```

---

## 4. Performance Benchmarking

A comparative benchmarking suite is located in the parent `store` package (`store_benchmark_test.go`), allowing you to measure Aerospike vs. Redis performance side-by-side in your environment.

To run the benchmarks, ensure both Redis and Aerospike are running locally:

```bash
go test -bench=. -benchmem ./cmd/servers/ateapi/store/...
```
