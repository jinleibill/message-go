package nats

import (
	"encoding/json"
	msg "github.com/jinleibill/message-go"
	"github.com/nats-io/nats.go/jetstream"
)

type Serializer interface {
	Serialize(message msg.Message) ([]byte, error)
	Deserialize(message jetstream.Msg) (msg.Message, error)
}

var DefaultSerializer = serializer{}

type serializer struct{}

func (s serializer) Serialize(message msg.Message) ([]byte, error) {
	jsonMsg := JsonMsg{
		Key:     message.Key(),
		Headers: message.Headers(),
		Payload: message.Payload(),
	}

	return json.Marshal(&jsonMsg)
}

func (s serializer) Deserialize(message jetstream.Msg) (msg.Message, error) {
	jsonMsg := JsonMsg{}
	err := json.Unmarshal(message.Data(), &jsonMsg)
	if err != nil {
		return msg.NewMessage([]byte("")), err
	}

	return msg.NewMessage(jsonMsg.Payload, msg.WithKey(jsonMsg.Key), msg.WithHeaders(jsonMsg.Headers)), nil
}

type JsonMsg struct {
	Key     string      `json:"key"`
	Headers msg.Headers `json:"headers"`
	Payload []byte      `json:"payload"`
}
