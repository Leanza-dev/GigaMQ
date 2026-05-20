package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"github.com/Leanza-dev/GigaMQ/internal/engine"
	"go.uber.org/zap"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// mockSubscriber is a simple Subscriber that collects received messages.
type mockSubscriber struct {
	id       string
	received []domain.Message
	mu       sync.Mutex
	closed   bool
}

func newMockSubscriber(id string) *mockSubscriber {
	return &mockSubscriber{id: id}
}

func (s *mockSubscriber) GetID() string { return s.id }

func (s *mockSubscriber) Send(msg *domain.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("subscriber %s is closed", s.id)
	}
	s.received = append(s.received, *msg)
	return nil
}

func (s *mockSubscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *mockSubscriber) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

// atomicCountSubscriber counts deliveries with an atomic counter (race-safe).
type atomicCountSubscriber struct {
	id      string
	counter *atomic.Int64
}

func (a *atomicCountSubscriber) GetID() string { return a.id }
func (a *atomicCountSubscriber) Send(_ *domain.Message) error {
	a.counter.Add(1)
	return nil
}
func (a *atomicCountSubscriber) Close() error { return nil }

// newTestEngine creates an Engine with a no-op logger suitable for unit tests.
func newTestEngine(workers, bufSize int) *Engine {
	logger := zap.NewNop()
	dispatcher := engine.NewDispatcher(workers, bufSize, logger)
	return NewEngine(workers, bufSize, dispatcher, logger)
}

// startTestEngine creates, starts, and returns an engine with a cancellable context.
func startTestEngine(t *testing.T, workers, buf int) (*Engine, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	eng := newTestEngine(workers, buf)
	eng.Start(ctx)
	return eng, cancel
}

// assertCount polls sub.Count() until it reaches `want` or `timeout` expires.
func assertCount(t *testing.T, sub *mockSubscriber, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sub.Count() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("assertCount: expected %d messages, got %d after %s", want, sub.Count(), timeout)
}

// ─── Test 1: Basic publish/subscribe delivery ─────────────────────────────────

func TestPublishDeliveredToSubscriber(t *testing.T) {
	t.Parallel()
	eng, cancel := startTestEngine(t, 4, 100)
	defer cancel()

	sub := newMockSubscriber("sub-1")
	eng.Subscribe("orders", sub)

	eng.Inbound <- &domain.Message{Topic: "orders", Payload: []byte("order-001")}

	assertCount(t, sub, 1, 500*time.Millisecond)
}

// ─── Test 2: Fan-out to multiple subscribers on the same topic ────────────────

func TestFanOutToMultipleSubscribers(t *testing.T) {
	t.Parallel()
	eng, cancel := startTestEngine(t, 4, 100)
	defer cancel()

	sub1 := newMockSubscriber("sub-a")
	sub2 := newMockSubscriber("sub-b")
	sub3 := newMockSubscriber("sub-c")
	eng.Subscribe("metrics", sub1)
	eng.Subscribe("metrics", sub2)
	eng.Subscribe("metrics", sub3)

	eng.Inbound <- &domain.Message{Topic: "metrics", Payload: []byte("cpu=82")}

	assertCount(t, sub1, 1, 500*time.Millisecond)
	assertCount(t, sub2, 1, 500*time.Millisecond)
	assertCount(t, sub3, 1, 500*time.Millisecond)
}

// ─── Test 3: Topic isolation ──────────────────────────────────────────────────

func TestTopicIsolation(t *testing.T) {
	t.Parallel()
	eng, cancel := startTestEngine(t, 4, 100)
	defer cancel()

	subOrders := newMockSubscriber("orders-sub")
	subMetrics := newMockSubscriber("metrics-sub")
	eng.Subscribe("orders", subOrders)
	eng.Subscribe("metrics", subMetrics)

	eng.Inbound <- &domain.Message{Topic: "orders", Payload: []byte("buy")}
	eng.Inbound <- &domain.Message{Topic: "orders", Payload: []byte("sell")}

	assertCount(t, subOrders, 2, 500*time.Millisecond)

	// metrics subscriber must receive ZERO messages
	time.Sleep(100 * time.Millisecond)
	if got := subMetrics.Count(); got != 0 {
		t.Errorf("topic isolation violated: metrics-sub received %d messages (expected 0)", got)
	}
}

// ─── Test 4: Unsubscribe stops delivery ───────────────────────────────────────

func TestUnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()
	eng, cancel := startTestEngine(t, 4, 100)
	defer cancel()

	sub := newMockSubscriber("ephemeral")
	eng.Subscribe("events", sub)

	eng.Inbound <- &domain.Message{Topic: "events", Payload: []byte("first")}
	assertCount(t, sub, 1, 500*time.Millisecond)

	eng.Unsubscribe("events", "ephemeral")
	eng.Inbound <- &domain.Message{Topic: "events", Payload: []byte("second")}

	time.Sleep(150 * time.Millisecond)
	if got := sub.Count(); got != 1 {
		t.Errorf("expected 1 message after unsubscribe, got %d", got)
	}
}

// ─── Test 5: Concurrent publish — no data races ───────────────────────────────
// Run with: go test ./internal/queue/... -v -race

func TestConcurrentPublishNoRace(t *testing.T) {
	t.Parallel()
	eng, cancel := startTestEngine(t, 16, 10000)
	defer cancel()

	const publishers = 50
	const msgsPerPublisher = 20
	expected := int64(publishers * msgsPerPublisher)

	var received atomic.Int64
	atomicSub := &atomicCountSubscriber{id: "concurrent-sub", counter: &received}
	eng.Subscribe("stress", atomicSub)

	var wg sync.WaitGroup
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for j := 0; j < msgsPerPublisher; j++ {
				eng.Inbound <- &domain.Message{
					Topic:   "stress",
					Payload: []byte(fmt.Sprintf("pub-%d-msg-%d", pid, j)),
				}
			}
		}(i)
	}
	wg.Wait()

	// Poll until all messages delivered or deadline exceeded
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() >= expected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := received.Load(); got != expected {
		t.Errorf("concurrency test: expected %d messages delivered, got %d", expected, got)
	}
}
