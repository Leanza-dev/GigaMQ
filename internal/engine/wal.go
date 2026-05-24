package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
	"go.uber.org/zap"
)

// WAL (Write-Ahead Log) provides persistent durability for messages.
// It uses a background flusher (Group Commit) to maximize throughput.
type WAL struct {
	file    *os.File
	writer  *bufio.Writer
	mu      sync.Mutex
	inbound chan *domain.Message
	done    chan struct{}
	wg      sync.WaitGroup
	logger  *zap.Logger
}

// NewWAL initializes the Write-Ahead Log file with a background flusher.
func NewWAL(filepath string, logger *zap.Logger) (*WAL, error) {
	// Removed O_SYNC to avoid blocking on every write. We rely on the background flusher.
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	w := &WAL{
		file:    file,
		writer:  bufio.NewWriterSize(file, 1024*1024), // 1MB buffer
		inbound: make(chan *domain.Message, 10000),
		done:    make(chan struct{}),
		logger:  logger,
	}

	w.wg.Add(1)
	go w.flusher()

	return w, nil
}

// flusher runs in the background and commits batches to disk
func (w *WAL) flusher() {
	defer w.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-w.inbound:
			if !ok {
				w.sync()
				return
			}
			w.writeBinary(msg)
		case <-ticker.C:
			w.sync()
		case <-w.done:
			w.sync()
			return
		}
	}
}

func (w *WAL) writeBinary(msg *domain.Message) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Binary Format: [TopicLen uint16][Topic][PayloadLen uint32][Payload]
	var header [6]byte
	topicLen := uint16(len(msg.Topic))
	payloadLen := uint32(len(msg.Payload))

	binary.LittleEndian.PutUint16(header[0:2], topicLen)
	binary.LittleEndian.PutUint32(header[2:6], payloadLen)

	w.writer.Write(header[:])
	w.writer.WriteString(msg.Topic)
	w.writer.Write(msg.Payload)
}

func (w *WAL) sync() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer.Buffered() > 0 {
		w.writer.Flush()
		w.file.Sync() // Actual fsync to disk
	}
}

// Append queues a message for asynchronous WAL commit.
func (w *WAL) Append(msg *domain.Message) error {
	select {
	case w.inbound <- msg:
		return nil
	case <-w.done:
		return fmt.Errorf("WAL is closed")
	}
}

// Close gracefully flushes remaining logs and closes the WAL.
func (w *WAL) Close() error {
	close(w.done)
	w.wg.Wait()
	close(w.inbound)
	return w.file.Close()
}
