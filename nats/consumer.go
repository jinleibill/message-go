package nats

import (
	"context"
	msg "github.com/jinleibill/message-go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"strings"
	"time"
)

var _ msg.Consumer = (*Consumer)(nil)

var DefaultAckWait = 30 * time.Second

type Consumer struct {
	opts       nats.Options
	conn       *nats.Conn
	js         jetstream.JetStream
	serializer Serializer
	logger     msg.Logger
	ackWait    time.Duration
}

func NewConsumer(servers []string, options ...ConsumerOption) *Consumer {
	opts := nats.GetDefaultOptions()
	opts.Servers = servers

	p := &Consumer{
		opts:       opts,
		serializer: DefaultSerializer,
		logger:     msg.DefaultLogger,
		ackWait:    DefaultAckWait,
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

func (c *Consumer) Listen(ctx context.Context, channel string, consumer msg.ReceiverFunc) error {
	streamName := strings.Replace(channel, ".", "_", -1)

	s, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{channel},
	})
	if err != nil {
		c.logger.Error("failed to create stream, err: %v", err)
		return err
	}

	cons, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   streamName,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	cc, err := cons.Consume(c.receiveMessage(ctx, consumer))
	if err != nil {
		return err
	}

	<-ctx.Done()
	cc.Stop()

	return nil
}

func (c *Consumer) Close(ctx context.Context) error {
	c.logger.Trace("closing nats jetstream Consumer")
	return c.conn.Drain()
}

func (c *Consumer) receiveMessage(ctx context.Context, consumer msg.ReceiverFunc) func(msg jetstream.Msg) {
	return func(jsMsg jetstream.Msg) {
		var err error

		var message msg.Message
		message, err = c.serializer.Deserialize(jsMsg)
		if err != nil {
			c.logger.Error("message failed to unmarshal: %v", err)
			return
		}

		wCtx, cancel := context.WithTimeout(ctx, c.ackWait)
		defer cancel()

		errC := make(chan error)
		go func() {
			errC <- consumer(wCtx, message)
		}()

		select {
		case err = <-errC:
			if err == nil {
				if ackErr := jsMsg.Ack(); ackErr != nil {
					c.logger.Error("failed to acknowledging messages, err: %v", ackErr)
				}
			}
		case <-ctx.Done():
			c.logger.Trace("listener has closed the message")
		case <-wCtx.Done():
			c.logger.Trace("timeout waiting for message consumer to finish")
		}
	}
}

type ConsumerOption func(*Consumer)

func WithConsumerNatsConn(options ...nats.Option) ConsumerOption {
	return func(p *Consumer) {
		for _, opt := range options {
			if opt != nil {
				if err := opt(&p.opts); err != nil {
					panic(err)
				}
			}
		}
	}
}

func WithConsumerLogger(logger msg.Logger) ConsumerOption {
	return func(p *Consumer) {
		p.logger = logger
	}
}

func WithConsumerSerializer(serializer Serializer) ConsumerOption {
	return func(p *Consumer) {
		p.serializer = serializer
	}
}
