# GigaMQ 🚀

**A high-performance, lightweight Message Queue engine built in Go, focusing on zero-copy processing and ultra-low latency.**

![Go](https://img.shields.io/badge/Language-Go-blue.svg)
![Concurrency](https://img.shields.io/badge/Pattern-Fan--Out-green.svg)
![Latency](https://img.shields.io/badge/Latency-Sub--ms-brightgreen.svg)

## 📌 Vision

GigaMQ is designed to handle massive volumes of events with minimal overhead. It leverages Go's native concurrency primitives (Goroutines and Channels) to implement a non-blocking dispatcher capable of routing thousands of messages per second.

### Key Features
- **Asynchronous Fan-out**: Distribute messages to multiple workers without blocking the main ingestor.
- **In-Memory Buffer**: High-speed circular buffers for burst handling.
- **TCP Front-end**: Custom protocol over TCP for minimal packet overhead.

## 🛠 Architecture

- `cmd/server/main.go`: Application entry point and signal handling.
- `internal/queue/engine.go`: The core logic of the queue, worker pools, and dispatching.
- `internal/network/tcp.go`: Handlers for incoming connections and message parsing.

## 🚀 Future Roadmap
- [ ] Disk Persistence (AOF - Append Only File)
- [ ] Message Acknowledgement (ACK)
- [ ] Consumer Groups

---
*Developed by Pedro Leanza for high-scale backend demonstration.*
