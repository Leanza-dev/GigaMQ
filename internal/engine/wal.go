package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"go.uber.org/zap"
)

// WAL (Write-Ahead Log) provides persistent durability for messages.
// It writes to disk synchronously (O_SYNC) to guarantee no data loss on crashes.
type WAL struct {
	file   *os.File
	mu     sync.Mutex
	logger *zap.Logger
}

// NewWAL initializes the Write-Ahead Log file.
func NewWAL(filepath string, logger *zap.Logger) (*WAL, error) {
	// O_SYNC enforces that each write flushes directly to the disk hardware.
	// This penalizes throughput but guarantees absolute durability.
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:   file,
		logger: logger,
	}, nil
}

// Append serializes a message to JSON and writes it synchronously to the log.
func (w *WAL) Append(msg *domain.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize to JSON for current human-readable debugging requirements.
	// In the future, this should be optimized to a custom binary format.
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Append newline to separate JSON objects
	data = append(data, '\n')

	_, err = w.file.Write(data)
	if err != nil {
		w.logger.Error("Failed to write to WAL disk", zap.Error(err))
		return fmt.Errorf("wal disk write failure: %w", err)
	}

	return nil
}

// Close gracefully closes the WAL file descriptor.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
