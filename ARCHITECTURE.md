# Engineering at Scale: GigaMQ Architecture

Welcome to the GigaMQ *Architecture Decision Record* (ADR). We designed this *Message Broker* with a single objective: to maximize *Throughput* while minimizing *Garbage Collection* pauses under extremely high pressure.

## 1. Zero-Copy Routing
In inexperienced brokers, fanning out a message with a large `[]byte` payload to 10,000 clients copies the struct in memory 10,000 times, frying RAM and choking the CPU with *GC Stop-The-World* pauses.
- **The Solution:** The entire messaging pipeline (Engine -> Topic -> Client) strictly routes `*domain.Message` pointers. Regardless of payload size, the broker allocates and moves only 8 bytes. Data immutability is guaranteed as an architectural premise.

## 2. Asynchronous Fan-out and O(1) Engine Unblocking
The central router (`Engine`) cannot get stuck waiting to iterate over thousands of subscribers during a *Broadcast*.
- **Active Defense (Implemented):** In the `Broadcast` method, we take a near-instantaneous *Snapshot* of the subscribers map and release the `RWMutex`. The *fan-out* itself (the `.Send()` calls) is dispatched to a dedicated background *Worker Goroutine*. The Engine returns to process the next message in O(1) time, eradicating any *Starvation*.

## 3. Resilience: Protection against Head-of-Line Blocking
If a slow consumer's bandwidth is saturated, its internal network buffer will fill up. If the broker tries to write to a blocked buffer, the entire *Goroutine* hangs.
- **The Solution:** The send calls (`c.outbound <- msg`) operate on non-blocking channels (`select { case ... default: }`). Slow consumers will have their messages actively ejected in favor of node survival and low latency for fast consumers. Additionally, we mitigate *Slowloris* attacks with rigid TCP *Deadlines* (dropping idle *Sockets* to prevent memory leaks (OOM)).
