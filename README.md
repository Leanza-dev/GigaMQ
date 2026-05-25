# GigaMQ 🚀

**A pub/sub message broker built from scratch in Go — no Kafka, no RabbitMQ, no frameworks. Just goroutines, channels, and mutexes.**

![Go](https://img.shields.io/badge/Language-Go-00ADD8.svg)
![Concurrency](https://img.shields.io/badge/Pattern-Fan--Out-green.svg)
![Latency](https://img.shields.io/badge/Latency-Sub--ms-brightgreen.svg)
![Docker](https://img.shields.io/badge/Infra-Docker--Compose-2496ED.svg)

---

## Why I built this

I was using Kafka in another project and realized I had no real idea what was happening underneath the client library. What does fan-out actually look like in code? Where does backpressure kick in? What breaks first when subscribers lag behind publishers?

So I built GigaMQ to find out.

The core mechanic is a buffered channel (10,000 capacity) that receives all incoming messages, fed into a worker pool that routes by topic to registered subscribers. Each subscriber gets its own non-blocking send — so a slow reader can't stall everyone else.

**The things I learned building this that I didn't get from reading docs:**
- When you take a write lock vs. a read lock on a shared map matters more than you think under concurrent load
- Goroutine cleanup on TCP disconnect is not automatic — you have to design for it explicitly
- Non-blocking sends to subscriber channels (the `select`/`default` pattern) are what actually prevents head-of-line blocking, not just buffering

---

## Architecture

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
│               └─────────────┘                        │
└──────────────────────────────────────────────────────┘
```

### Engine (`internal/queue/engine.go`)

The `Engine` holds an `Inbound` buffered channel and a worker pool. Workers read from `Inbound` and route to the `Topic` map — guarded by `sync.RWMutex` for concurrent read access on the hot path.

### Fan-Out (`internal/queue/topic.go`)

Each topic broadcasts to all subscribers under a read lock. Each subscriber send is non-blocking (`select`/`default`) — if the subscriber's outbound buffer is full, the message is dropped with a logged error. Slow consumers don't block the publisher.

### TCP Protocol (`internal/protocol/`, `internal/network/`)

A simple wire protocol, testable with `netcat`:

```
PUB <topic>\n<payload>\n
SUB <topic>\n
```

---

## Getting Started

```bash
git clone https://github.com/Leanza-dev/GigaMQ.git
cd GigaMQ
docker compose up --build
```

Or locally:

```bash
go run ./cmd/server/
```

Server starts on port **9000**.

```bash
# Subscribe
echo -e "SUB orders\n" | nc localhost 9000

# Publish
echo -e "PUB orders\nhello world\n" | nc localhost 9000
```

---

## Project Structure

```
GigaMQ/
├── cmd/server/
│   └── main.go                  # Entry point, graceful shutdown
├── internal/
│   ├── domain/
│   │   └── message.go           # Message struct + Subscriber interface
│   ├── network/
│   │   ├── tcp_server.go        # TCP listener and connection handler
│   │   └── client.go            # TCP client implementing Subscriber
│   ├── protocol/
│   │   └── parser.go            # Wire protocol parser
│   └── queue/
│       ├── engine.go            # Worker pool + topic routing
│       ├── topic.go             # Fan-out broadcaster
│       └── engine_test.go       # Concurrency tests with -race flag
├── go.mod
└── docker-compose.yml
```

---

## Tests

```bash
go test ./internal/queue/... -v -race
```

The `-race` flag is the point — all tests pass clean under Go's data race detector.

---

## Roadmap

- [ ] Message persistence (WAL)
- [ ] ACK/NACK protocol
- [ ] Consumer groups with offset tracking
- [ ] Prometheus metrics endpoint
- [ ] Slow consumer detection
