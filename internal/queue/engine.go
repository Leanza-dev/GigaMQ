package queue

import (
	"context"
	"sync"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"go.uber.org/zap"
)

type Engine struct {
	Inbound chan domain.Message
	Workers int
	topics  map[string]*Topic
	mu      sync.RWMutex
	logger  *zap.Logger
	wg      sync.WaitGroup
}

func NewEngine(workers int, bufferSize int, logger *zap.Logger) *Engine {
	return &Engine{
		Inbound: make(chan domain.Message, bufferSize),
		Workers: workers,
		topics:  make(map[string]*Topic),
		logger:  logger,
	}
}

func (e *Engine) Start(ctx context.Context) {
	e.logger.Info("Starting GigaMQ Engine", zap.Int("workers", e.Workers))
	for i := 0; i < e.Workers; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}
}

func (e *Engine) worker(ctx context.Context, id int) {
	defer e.wg.Done()
	e.logger.Debug("Worker started", zap.Int("worker_id", id))
	
	for {
		select {
		case msg := <-e.Inbound:
			e.routeMessage(msg)
		case <-ctx.Done():
			e.logger.Debug("Worker shutting down", zap.Int("worker_id", id))
			return
		}
	}
}

func (e *Engine) getOrCreateTopic(name string) *Topic {
	e.mu.RLock()
	t, exists := e.topics[name]
	e.mu.RUnlock()

	if exists {
		return t
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if t, exists = e.topics[name]; exists {
		return t
	}

	t = NewTopic(name, e.logger)
	e.topics[name] = t
	return t
}

func (e *Engine) Subscribe(topicName string, sub domain.Subscriber) {
	t := e.getOrCreateTopic(topicName)
	t.AddSubscriber(sub)
}

func (e *Engine) Unsubscribe(topicName string, subID string) {
	e.mu.RLock()
	t, exists := e.topics[topicName]
	e.mu.RUnlock()
	if exists {
		t.RemoveSubscriber(subID)
	}
}

func (e *Engine) routeMessage(msg domain.Message) {
	e.mu.RLock()
	t, exists := e.topics[msg.Topic]
	e.mu.RUnlock()

	if exists {
		t.Broadcast(msg)
	} else {
		e.logger.Warn("Message routed to topic with no subscribers", zap.String("topic", msg.Topic))
	}
}

func (e *Engine) Stop() {
	e.wg.Wait()
	e.logger.Info("GigaMQ Engine fully stopped")
}
