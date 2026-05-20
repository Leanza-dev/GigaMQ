package engine

import (
	"context"
	"sync"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"go.uber.org/zap"
)

// DispatchTask wraps a message and a list of targets for a worker to process.
type DispatchTask struct {
	Message *domain.Message
	Targets []domain.Subscriber
}

// Dispatcher manages a high-performance worker pool to fan-out messages.
type Dispatcher struct {
	TaskQueue chan DispatchTask
	Workers   int
	wg        sync.WaitGroup
	logger    *zap.Logger
}

// NewDispatcher creates a new worker pool dispatcher to avoid unbounded goroutine explosion.
func NewDispatcher(workers int, queueSize int, logger *zap.Logger) *Dispatcher {
	return &Dispatcher{
		TaskQueue: make(chan DispatchTask, queueSize),
		Workers:   workers,
		logger:    logger,
	}
}

// Start boots up the worker pool.
func (d *Dispatcher) Start(ctx context.Context) {
	d.logger.Info("Starting Dispatcher Worker Pool", zap.Int("workers", d.Workers))
	for i := 0; i < d.Workers; i++ {
		d.wg.Add(1)
		go d.worker(ctx, i)
	}
}

// worker constantly pulls fan-out tasks and processes them safely.
func (d *Dispatcher) worker(ctx context.Context, id int) {
	defer d.wg.Done()
	for {
		select {
		case task, ok := <-d.TaskQueue:
			if !ok {
				return
			}
			for _, sub := range task.Targets {
				// Each subscriber handles its own non-blocking send (Head-of-Line protection)
				if err := sub.Send(task.Message); err != nil {
					d.logger.Error("Failed to send message to subscriber", zap.String("sub_id", sub.GetID()), zap.Error(err))
				}
			}
		case <-ctx.Done():
			d.logger.Debug("Dispatcher worker shutting down", zap.Int("worker_id", id))
			return
		}
	}
}

// Submit enqueues a fan-out task. It does not block unless the TaskQueue is full.
func (d *Dispatcher) Submit(msg *domain.Message, targets []domain.Subscriber) {
	select {
	case d.TaskQueue <- DispatchTask{Message: msg, Targets: targets}:
		// Successfully queued
	default:
		d.logger.Warn("Dispatcher queue full, dropping broadcast! Consider scaling workers or queue size.", zap.String("topic", msg.Topic))
	}
}

// Stop waits for all workers to cleanly shut down.
func (d *Dispatcher) Stop() {
	close(d.TaskQueue)
	d.wg.Wait()
	d.logger.Info("Dispatcher Worker Pool fully stopped")
}
