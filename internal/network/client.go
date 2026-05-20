package network

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
)

type Client struct {
	id       string
	conn     net.Conn
	outbound chan *domain.Message
	done     chan struct{}
}

func NewClient(id string, conn net.Conn) *Client {
	c := &Client{
		id:       id,
		conn:     conn,
		outbound: make(chan *domain.Message, 256),
		done:     make(chan struct{}),
	}
	go c.writePump()
	return c
}

func (c *Client) writePump() {
	writer := bufio.NewWriter(c.conn)
	for {
		select {
		case msg, ok := <-c.outbound:
			if !ok {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			header := fmt.Sprintf("%s %d\r\n", msg.Topic, len(msg.Payload))
			
			if _, err := writer.WriteString(header); err != nil {
				return
			}
			if _, err := writer.Write(msg.Payload); err != nil {
				return
			}
			if _, err := writer.WriteString("\r\n"); err != nil {
				return
			}
			if err := writer.Flush(); err != nil {
				return
			}
			c.conn.SetWriteDeadline(time.Time{})
		case <-c.done:
			return
		}
	}
}

func (c *Client) GetID() string {
	return c.id
}

func (c *Client) Send(msg *domain.Message) error {
	select {
	case c.outbound <- msg:
		return nil
	default:
		return fmt.Errorf("client %s buffer full, dropping message (Head-of-Line protection)", c.id)
	}
}

func (c *Client) Close() error {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
	return c.conn.Close()
}
