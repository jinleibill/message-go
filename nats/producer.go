package nats

import (
	"context"
	msg "github.com/jinleibill/message-go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"strings"
)

type Producer struct {
	opts       nats.Options
	conn       *nats.Conn
	js         jetstream.JetStream
	serializer Serializer
	logger     msg.Logger
}

func NewProducer(servers []string, options ...ProducerOption) *Producer {
	opts := nats.GetDefaultOptions()
	opts.Servers = servers

	p := &Producer{
		opts:       opts,
		serializer: DefaultSerializer,
		logger:     msg.DefaultLogger,
	}

	for _, option := range options {
		option(p)
	}

	nc, err := p.opts.Connect()
	if err != nil {
		panic(err)
	}
	p.conn = nc

	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	p.js = js

	return p
}

func (p *Producer) Send(ctx context.Context, channel string, message msg.Message) error {
	natsMsg, err := p.serializer.Serialize(message)
	if err != nil {
		p.logger.Error("failed to marshal message, err: %v", err)
		return err
	}

	_, err = p.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     strings.Replace(channel, ".", "_", -1),
		Subjects: []string{channel},
	})
	if err != nil {
		p.logger.Error("failed to create stream, err: %v", err)
		return err
	}

	_, err = p.js.Publish(ctx, channel, natsMsg)
	return err
}

func (p *Producer) Close(ctx context.Context) error {
	p.logger.Trace("closing nats jetstream producer")
	return p.conn.Drain()
}

type ProducerOption func(*Producer)

func WithProducerNatsConn(options ...nats.Option) ProducerOption {
	return func(p *Producer) {
		for _, opt := range options {
			if opt != nil {
				if err := opt(&p.opts); err != nil {
					panic(err)
				}
			}
		}
	}
}

func WithProducerLogger(logger msg.Logger) ProducerOption {
	return func(p *Producer) {
		p.logger = logger
	}
}

func WithProducerSerializer(serializer Serializer) ProducerOption {
	return func(p *Producer) {
		p.serializer = serializer
	}
}
