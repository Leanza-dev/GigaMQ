package queue

import (
	"sync"
	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"go.uber.org/zap"
)

type Topic struct {
	Name        string
	subscribers map[string]domain.Subscriber
	mu          sync.RWMutex
	logger      *zap.Logger
}

func NewTopic(name string, logger *zap.Logger) *Topic {
	return &Topic{
		Name:        name,
		subscribers: make(map[string]domain.Subscriber),
		logger:      logger,
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

func (t *Topic) Broadcast(msg domain.Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	// Fan-out: send message to all subscribers
	for _, sub := range t.subscribers {
		if err := sub.Send(msg); err != nil {
			t.logger.Error("Failed to send message to subscriber", zap.String("sub_id", sub.GetID()), zap.Error(err))
		}
	}
}
