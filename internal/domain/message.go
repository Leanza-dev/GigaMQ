package domain

// Message represents a payload published to a topic.
type Message struct {
	Topic   string
	Payload []byte
}

// Subscriber defines an interface for any client that can receive messages.
type Subscriber interface {
	GetID() string
	Send(msg *Message) error
	Close() error
}
