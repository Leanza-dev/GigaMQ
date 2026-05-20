package network

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Leanza-dev/GigaMQ/internal/protocol"
	"github.com/Leanza-dev/GigaMQ/internal/queue"
	"go.uber.org/zap"
)

type TCPServer struct {
	addr   string
	engine *queue.Engine
	logger *zap.Logger
	wg     sync.WaitGroup
}

func NewTCPServer(addr string, engine *queue.Engine, logger *zap.Logger) *TCPServer {
	return &TCPServer{
		addr:   addr,
		engine: engine,
		logger: logger,
	}
}

func (s *TCPServer) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.logger.Info("TCP Server listening", zap.String("addr", s.addr))

	go func() {
		<-ctx.Done()
		s.logger.Info("TCP Server shutting down listener...")
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil // Graceful shutdown
			}
			s.logger.Error("Failed to accept connection", zap.Error(err))
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()

	clientID := fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano())
	s.logger.Debug("New connection accepted", zap.String("client_id", clientID))

	client := NewClient(clientID, conn)
	defer client.Close()
	
	reader := bufio.NewReader(conn)
	
	subscriptions := make([]string, 0)
	defer func() {
		for _, topic := range subscriptions {
			s.engine.Unsubscribe(topic, clientID)
		}
	}()

	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		cmd, err := protocol.ParseCommand(reader)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				s.logger.Warn("Client timed out (Slowloris protection)", zap.String("client_id", clientID))
			} else if err == io.EOF {
				s.logger.Debug("Client disconnected", zap.String("client_id", clientID))
			} else {
				s.logger.Warn("Failed to parse command", zap.String("client_id", clientID), zap.Error(err))
			}
			return
		}
		conn.SetReadDeadline(time.Time{})

		switch cmd.Type {
		case protocol.CmdPub:
			ctxTimeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			select {
			case s.engine.Inbound <- &cmd.Message:
				cancel()
			case <-ctxTimeout.Done():
				cancel()
				s.logger.Warn("Engine buffer full, rejecting message (Backpressure)", zap.String("client_id", clientID), zap.String("topic", cmd.Message.Topic))
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				conn.Write([]byte("ERR_BUFFER_FULL\n"))
				conn.SetWriteDeadline(time.Time{})
			}
		case protocol.CmdSub:
			s.engine.Subscribe(cmd.Message.Topic, client)
			subscriptions = append(subscriptions, cmd.Message.Topic)
			s.logger.Debug("Client subscribed to topic", zap.String("client_id", clientID), zap.String("topic", cmd.Message.Topic))
		}
	}
}

func (s *TCPServer) Wait() {
	s.wg.Wait()
	s.logger.Info("All TCP connections closed")
}
