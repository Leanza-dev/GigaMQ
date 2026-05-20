# Engineering at Scale: GigaMQ Architecture

Welcome to the GigaMQ Architecture Decision Record (ADR). We designed this Message Broker with a singular objective: to maximize throughput while minimizing Garbage Collection (GC) pauses under extreme pressure.

## 1. Zero-Copy Routing
In amateur brokers, fanning out a message containing a large `[]byte` payload to 10,000 clients copies the memory struct 10,000 times, frying the RAM and choking the CPU with GC Stop-The-World pauses.
- **The Solution:** The entire messaging pipeline (Engine -> Topic -> Client) strictly traffics `*domain.Message` pointers. Regardless of the payload size, the broker allocates and moves a mere 8 bytes per fan-out. Data immutability is enforced as a foundational architectural premise.

## 2. Asynchronous Fan-out and O(1) Engine Unblocking
The central router (`Engine`) cannot afford to be trapped iterating through thousands of subscribers during a Broadcast event.
- **Active Defense (Implemented):** Within the `Broadcast` method, we perform a near-instantaneous Snapshot of the subscriber map and immediately release the `RWMutex`. The fan-out itself (invoking `.Send()`) is dispatched to a dedicated background Worker Goroutine. The Engine resumes processing the subsequent message in O(1) time, eradicating any form of starvation.

## 3. Resilience: Head-of-Line Blocking Protection
If a slow consumer saturates its bandwidth, its internal network buffer overflows. If the broker attempts to write to a blocked buffer, the entire Goroutine stalls.
- **The Solution:** Outbound send calls (`c.outbound <- msg`) operate on non-blocking channels (`select { case ... default: }`). Slow consumers have their messages actively ejected to preserve the node's survival and guarantee sub-millisecond latencies for fast consumers. Additionally, we mitigate Slowloris attacks with rigid TCP Deadlines (proactively dropping idle sockets to prevent Out-Of-Memory (OOM) leaks).
