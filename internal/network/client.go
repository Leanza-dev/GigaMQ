package network

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
)

type Client struct {
	id   string
	conn net.Conn
}

func NewClient(id string, conn net.Conn) *Client {
	return &Client{
		id:   id,
		conn: conn,
	}
}

func (c *Client) GetID() string {
	return c.id
}

func (c *Client) Send(msg domain.Message) error {
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	defer c.conn.SetWriteDeadline(time.Time{})

	header := fmt.Sprintf("%s %d\r\n", msg.Topic, len(msg.Payload))
	
	writer := bufio.NewWriter(c.conn)
	if _, err := writer.WriteString(header); err != nil {
		return err
	}
	if _, err := writer.Write(msg.Payload); err != nil {
		return err
	}
	if _, err := writer.WriteString("\r\n"); err != nil {
		return err
	}
	return writer.Flush()
}

func (c *Client) Close() error {
	return c.conn.Close()
}
