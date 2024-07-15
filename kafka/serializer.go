package kafka

import (
	msg "github.com/jinleibill/message-go"
	"github.com/segmentio/kafka-go"
)

type Serializer interface {
	Serialize(message msg.Message) (kafka.Message, error)
	Deserialize(message kafka.Message) (msg.Message, error)
}

var _ Serializer = (*serializer)(nil)

var DefaultSerializer = serializer{}

type serializer struct{}

func (s serializer) Serialize(message msg.Message) (kafka.Message, error) {
	headers := make([]kafka.Header, 0, len(message.Headers()))

	for key, value := range message.Headers() {
		headers = append(headers, kafka.Header{
			Key:   key,
			Value: []byte(value),
		})
	}

	kafkaMsg := kafka.Message{
		Value:   message.Payload(),
		Headers: headers,
	}

	if message.Key() != "" {
		kafkaMsg.Key = []byte(message.Key())
	}

	return kafkaMsg, nil
}

func (s serializer) Deserialize(message kafka.Message) (msg.Message, error) {
	headers := make(msg.Headers, len(message.Headers))

	for _, header := range message.Headers {
		headers.Set(header.Key, string(header.Value))
	}

	return msg.NewMessage(message.Value, msg.WithKey(string(message.Key)), msg.WithHeaders(headers)), nil
}
