# CLAUDE.md - GigaMQ AI Guide

This document establishes the governance, architectural, and developmental guidelines for any AI agent operating within the **GigaMQ** repository. As the Tech Lead of this project, absolute compliance with the rules below is mandatory.

---

## 🏗️ Architectural Guidelines (Go Concurrency)

**GigaMQ** is a concurrent messaging engine in Go designed for extreme performance, ultra-high message throughput, and extremely low latency.

*   **Concurrency Patterns:** Structured use of `Goroutines`, `Channels`, and synchronization primitives from the `sync` package.
*   **Worker Pools:** Work channels (`Worker Pools`) to manage and limit concurrent processing of topics and queues.
*   **Thread Management:** Focus on maintaining thread-safe structures without blocking the Go scheduler runtime.

---

## 🚫 Unbreakable Rules (Guardrails)

1.  **Strict Prohibition of Goroutine Leaks:** Every spawned goroutine must have a strict lifecycle and a well-defined channel or context (`context.Context`) for safe cancellation and termination.
2.  **Unconditional Thread-Safety:** Concurrent access to maps or message topic buffers MUST be protected by `sync.Mutex` or `sync.RWMutex`. Never expose unprotected simultaneous read/write access.
3.  **Deadlock Prevention:** Always release Mutexes using `defer mutex.Unlock()` immediately after acquisition if the flow is complex, ensuring prevention against panics and deadlocks.
4.  **No Busy-Waiting:** Empty loops waiting for message arrivals are prohibited. Use channel selections (`select { case <-ch: }`) or condition variables (`sync.Cond`).

---

## 🛠️ Frequent Commands

*   **Build:** `go build -v ./...`
*   **Run Server:** `go run cmd/server/main.go`
*   **Run Tests:** `go test -v ./...`
*   **Run Race Test:** `go test -race ./...` (essential before any commit!)
*   **Format:** `go fmt ./...`
*   **Linting:** `golangci-lint run` (if available) or `go vet ./...`
