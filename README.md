# GigaMQ 🚀

> 🇺🇸 English | [🇧🇷 Português](./README.pt-BR.md)

**A high-performance, lightweight Message Queue engine built in Go — zero external brokers, sub-millisecond latency.**

![Go](https://img.shields.io/badge/Language-Go-00ADD8.svg)
![Concurrency](https://img.shields.io/badge/Pattern-Fan--Out-green.svg)
![Latency](https://img.shields.io/badge/Latency-Sub--ms-brightgreen.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

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
- **`Inbound` channel**: buffered, receives all published messages.
- **Worker pool**: N goroutines consume from `Inbound` concurrently.
- **Topic map**: `map[string]*Topic` guarded by `sync.RWMutex` with double-checked locking for lock-free reads.

### Topic & Fan-Out (`internal/queue/topic.go`)

Each topic maintains a `map[string]Subscriber` of active connections. On `Broadcast`, the topic iterates all subscribers under a read lock — zero blocking on publishing side.

### TCP Protocol (`internal/protocol/`, `internal/network/`)

A simple, human-readable wire protocol:

```
PUB <topic>\n<payload>\n
SUB <topic>\n
```

The `TCPServer` accepts connections and dispatches commands to the engine. Each subscriber connection is wrapped in a `Client` struct implementing the `Subscriber` interface.

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

The `-race` flag enables Go's built-in data race detector — all tests pass clean.

---

## Roadmap

- [ ] Message persistence (Append-Only File / WAL)
- [ ] Message acknowledgement (ACK/NACK)
- [ ] Consumer groups with offset tracking
- [ ] Prometheus metrics endpoint

---

*Developed by Pedro Leanza — High-Performance Backend & Distributed Systems.*
