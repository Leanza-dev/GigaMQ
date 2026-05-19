package protocol

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/Leanza-dev/GigaMQ/internal/domain"
)

var (
	ErrInvalidProtocol = errors.New("invalid protocol format")
	ErrUnknownCommand  = errors.New("unknown command")
)

type CommandType int

const (
	CmdPub CommandType = iota
	CmdSub
)

type Command struct {
	Type    CommandType
	Message domain.Message
}

// ParseCommand reads from the reader and parses PUB or SUB commands.
func ParseCommand(reader *bufio.Reader) (Command, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return Command{}, err
	}
	line = strings.TrimSpace(line)
	parts := strings.Split(line, " ")

	if len(parts) == 0 {
		return Command{}, ErrInvalidProtocol
	}

	switch strings.ToUpper(parts[0]) {
	case "PUB":
		if len(parts) != 3 {
			return Command{}, ErrInvalidProtocol
		}
		topic := parts[1]
		length, err := strconv.Atoi(parts[2])
		if err != nil || length <= 0 {
			return Command{}, ErrInvalidProtocol
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return Command{}, err
		}
		
		// Consume the trailing \r\n
		_, _ = reader.ReadBytes('\n')

		return Command{
			Type: CmdPub,
			Message: domain.Message{
				Topic:   topic,
				Payload: payload,
			},
		}, nil

	case "SUB":
		if len(parts) != 2 {
			return Command{}, ErrInvalidProtocol
		}
		return Command{
			Type: CmdSub,
			Message: domain.Message{
				Topic: parts[1],
			},
		}, nil

	default:
		return Command{}, ErrUnknownCommand
	}
}
