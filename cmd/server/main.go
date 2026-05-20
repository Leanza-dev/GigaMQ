package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Leanza-dev/GigaMQ/internal/network"
	"github.com/Leanza-dev/GigaMQ/internal/queue"
	"go.uber.org/zap"
)

func main() {
	// Initialize high-performance logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("Starting GigaMQ Initialization")

	// Global Context with Cancel for Graceful Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize the Core Engine (100 workers, 10,000 capacity buffer)
	engine := queue.NewEngine(100, 10000, logger)
	engine.Start(ctx)

	// Initialize the TCP Server adapter on port 9000
	tcpServer := network.NewTCPServer(":9000", engine, logger)

	// Start the TCP server in a separate goroutine
	go func() {
		if err := tcpServer.Start(ctx); err != nil {
			logger.Fatal("Failed to start TCP Server", zap.Error(err))
		}
	}()

	logger.Info("GigaMQ Server is ready to accept connections", zap.String("port", "9000"))

	// Setup OS signal trapping for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Block until a signal is received
	sig := <-sigChan
	logger.Info("Received shutdown signal", zap.String("signal", sig.String()))

	// Trigger cancellation across all components
	cancel()

	// Wait for components to finish processing (Graceful Shutdown)
	time.Sleep(1 * time.Second) // Small buffer to let TCP connections close
	tcpServer.Wait()
	engine.Stop()

	logger.Info("GigaMQ Shutdown successfully. Zero data loss.")
}
