# GigaMQ 🚀

> 🇺🇸 English | [🇧🇷 Português](./README.pt-BR.md)

📚 **Deep Dive Technical Resources:**
*   [**System Architecture Blueprint (ARCHITECTURE.md)**](./ARCHITECTURE.md) — Pub/sub dispatch engine, locking models, backpressure strategy.
*   [**Governança & Manual da IA (CLAUDE.md)**](./CLAUDE.md) — Regras corporativas, guardrails de concorrência e governança de IA.

**An advanced architectural case study in high-throughput messaging — building a pub/sub broker from scratch in Go to master real concurrency, backpressure control, and fan-out dispatch without external dependencies.**

![Go](https://img.shields.io/badge/Language-Go-00ADD8.svg)
![Concurrency](https://img.shields.io/badge/Pattern-Fan--Out-green.svg)
![Latency](https://img.shields.io/badge/Latency-Sub--ms-brightgreen.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

---

## 🎯 Engineering Objectives

Kafka and RabbitMQ are powerful tools — but using them doesn't teach you what's happening underneath. This project was built to understand those fundamentals directly: what does a real fan-out dispatcher look like? How do you handle backpressure when subscribers are slower than publishers? What breaks first under concurrent load?

**Core challenges explored:**
- **Backpressure**: A buffered channel with 10,000 capacity acts as the primary pressure valve. What happens when it fills? How do you detect slow consumers before the system degrades?
- **Lock contention**: The `sync.RWMutex` on the topic map allows concurrent reads at zero cost. Understanding *when* to take a write lock vs. a read lock is the real lesson here.
- **Goroutine lifecycle**: Each subscriber connection spawns a goroutine. What happens to that goroutine when the TCP connection drops? How do you guarantee cleanup without leaks?
- **Fan-out fairness**: When a topic has 100 subscribers, how does the dispatcher ensure no single slow subscriber blocks delivery to others?

> Built as a self-directed study in Go concurrency — going well beyond basic goroutines into real systems-level thinking.

---

## Architecture Overview

GigaMQ is a pub/sub message broker built entirely on Go's native concurrency primitives — no Kafka, no RabbitMQ, no external dependencies. It exposes a **custom TCP protocol** and implements a **fan-out dispatcher** backed by a buffered channel worker pool.

```
┌──────────────────────────────────────────────────────┐
│                    GigaMQ SERVER                     │
│                                                      │
│  TCP Clients                                         │
│  PUB orders → ┌─────────────┐                       │
│               │  Inbound    │  buffered chan          │
│               │  Channel    │ (10,000 capacity)      │
│               └──────┬──────┘                        │
│                      │  fan-out                      │
│         ┌────────────┼────────────┐                  │
│         ▼            ▼            ▼                  │
│     Worker 1     Worker 2    Worker N                │
│         │            │            │                  │
│         └────────────┼────────────┘                  │
│                      │  route by topic               │
│               ┌──────▼──────┐                        │
│               │ Topic Map   │  (RWMutex-guarded)     │
│               │ ┌─────────┐ │                        │
│               │ │ orders  │→│ Subscriber A           │
│               │ │ metrics │→│ Subscriber B, C        │
│               │ └─────────┘ │                        │
│               └─────────────┘                        │
└──────────────────────────────────────────────────────┘
```

### Engine (`internal/queue/engine.go`)

The core of GigaMQ. An `Engine` struct holds:
- **`Inbound` channel**: buffered, receives all published messages. First line of backpressure defense.
- **Worker pool**: N goroutines consume from `Inbound` concurrently — horizontal scaling by simply increasing pool size.
- **Topic map**: `map[string]*Topic` guarded by `sync.RWMutex` with double-checked locking for lock-free reads on the hot path.

### Topic & Fan-Out (`internal/queue/topic.go`)

Each topic maintains a `map[string]Subscriber` of active connections. On `Broadcast`, the topic iterates all subscribers under a read lock — zero blocking on the publishing side. Each subscriber delivery is non-blocking to prevent a slow reader from stalling others.

### TCP Protocol (`internal/protocol/`, `internal/network/`)

A simple, human-readable wire protocol designed to be testable with `netcat`:

```
PUB <topic>\n<payload>\n
SUB <topic>\n
```

The `TCPServer` accepts connections and dispatches commands to the engine. Each subscriber connection is wrapped in a `Client` struct implementing the `Subscriber` interface — clean inversion of control.

---

## Getting Started

### Run with Docker (Recommended)

```bash
git clone https://github.com/Leanza-dev/GigaMQ.git
cd GigaMQ
docker compose up --build
```

### Run Locally

```bash
go run ./cmd/server/
```

The server starts listening on **port 9000**.

### Connect a Client

```bash
# Subscribe to a topic
echo -e "SUB orders\n" | nc localhost 9000

# Publish a message
echo -e "PUB orders\nhello world\n" | nc localhost 9000
```

---

## Project Structure

```
GigaMQ/
├── cmd/server/
│   └── main.go                  # Entry point — logger, engine, TCP server, graceful shutdown
├── internal/
│   ├── domain/
│   │   └── message.go           # Message struct + Subscriber interface
│   ├── network/
│   │   ├── tcp_server.go        # TCP listener, connection handler, pub/sub dispatch
│   │   └── client.go            # TCP client implementing Subscriber interface
│   ├── protocol/
│   │   └── parser.go            # Wire protocol parser (PUB/SUB commands)
│   └── queue/
│       ├── engine.go            # Core dispatcher: worker pool + topic routing
│       ├── topic.go             # Fan-out broadcaster with RWMutex
│       └── engine_test.go       # Unit tests: pub/sub correctness + concurrency
├── go.mod
└── docker-compose.yml
```

---

## Running Tests

```bash
go test ./internal/queue/... -v -race
```

The `-race` flag enables Go's built-in **data race detector** — all tests pass clean. Verifying correctness under concurrent access was a primary engineering goal, not an afterthought.

---

## Roadmap

- [ ] Message persistence (Append-Only File / WAL)
- [ ] Message acknowledgement (ACK/NACK)
- [ ] Consumer groups with offset tracking
- [ ] Prometheus metrics endpoint
- [ ] Slow consumer detection and automatic backpressure signaling

---

*Pedro Leanza — CS Student · AI-Augmented Engineering · High-Performance Backend*
