package message_go

type Message interface {
	Key() string
	Headers() Headers
	Payload() []byte
}

var _ Message = (*message)(nil)

type message struct {
	key     string
	headers Headers
	payload []byte
}

func NewMessage(payload []byte, options ...MessageOption) Message {
	m := &message{
		payload: payload,
	}

	for _, option := range options {
		option(m)
	}

	return m
}

func (m *message) Key() string {
	return m.key
}

func (m *message) Headers() Headers {
	return m.headers
}

func (m *message) Payload() []byte {
	return m.payload
}

type MessageOption func(m *message)

func WithKey(key string) MessageOption {
	return func(m *message) {
		m.key = key
	}
}

func WithHeaders(headers Headers) MessageOption {
	return func(m *message) {
		m.headers = headers
	}
}
