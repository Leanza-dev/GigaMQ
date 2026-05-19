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
	defer conn.Close()

	clientID := fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano())
	s.logger.Debug("New connection accepted", zap.String("client_id", clientID))

	client := NewClient(clientID, conn)
	reader := bufio.NewReader(conn)
	
	subscriptions := make([]string, 0)
	defer func() {
		for _, topic := range subscriptions {
			s.engine.Unsubscribe(topic, clientID)
		}
	}()

	for {
		cmd, err := protocol.ParseCommand(reader)
		if err != nil {
			if err == io.EOF {
				s.logger.Debug("Client disconnected", zap.String("client_id", clientID))
			} else {
				s.logger.Warn("Failed to parse command", zap.String("client_id", clientID), zap.Error(err))
			}
			return
		}

		switch cmd.Type {
		case protocol.CmdPub:
			select {
			case s.engine.Inbound <- cmd.Message:
			default:
				s.logger.Warn("Engine buffer full, dropping message", zap.String("topic", cmd.Message.Topic))
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
