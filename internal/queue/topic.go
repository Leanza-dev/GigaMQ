package queue

import (
	"sync"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"github.com/Leanza-dev/GigaMQ/internal/engine"
	"go.uber.org/zap"
)

type Topic struct {
	Name        string
	subscribers map[string]domain.Subscriber
	mu          sync.RWMutex
	logger      *zap.Logger
	dispatcher  *engine.Dispatcher
}

func NewTopic(name string, dispatcher *engine.Dispatcher, logger *zap.Logger) *Topic {
	return &Topic{
		Name:        name,
		subscribers: make(map[string]domain.Subscriber),
		logger:      logger,
		dispatcher:  dispatcher,
	}
}

func (t *Topic) AddSubscriber(sub domain.Subscriber) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribers[sub.GetID()] = sub
	t.logger.Debug("Subscriber added to topic", zap.String("topic", t.Name), zap.String("sub_id", sub.GetID()))
}

func (t *Topic) RemoveSubscriber(subID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.subscribers, subID)
	t.logger.Debug("Subscriber removed from topic", zap.String("topic", t.Name), zap.String("sub_id", subID))
}

func (t *Topic) Broadcast(msg *domain.Message) {
	t.mu.RLock()
	// Fast snapshot of subscribers to release the RLock immediately.
	// Prevents starvation of new subscribers and frees the Engine.
	subs := make([]domain.Subscriber, 0, len(t.subscribers))
	for _, sub := range t.subscribers {
		subs = append(subs, sub)
	}
	t.mu.RUnlock()

	// Delegate fan-out to the bounded high-performance worker pool
	t.dispatcher.Submit(msg, subs)
}
