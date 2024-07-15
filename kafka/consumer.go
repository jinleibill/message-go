package kafka

import (
	"context"
	"errors"
	msg "github.com/jinleibill/message-go"
	"github.com/segmentio/kafka-go"
	"io"
	"time"
)

var _ msg.Consumer = (*Consumer)(nil)

var DefaultAckWait = 30 * time.Second

type Consumer struct {
	readerCfg  kafka.ReaderConfig
	serializer Serializer
	logger     msg.Logger
	ackWait    time.Duration
}

func NewConsumer(brokers []string, groupID string, options ...ConsumerOption) *Consumer {
	c := &Consumer{
		readerCfg: kafka.ReaderConfig{
			Brokers: brokers,
			GroupID: groupID,
			Dialer:  kafka.DefaultDialer,
		},
		serializer: DefaultSerializer,
		logger:     msg.DefaultLogger,
		ackWait:    DefaultAckWait,
	}

	for _, option := range options {
		option(c)
	}

	return c
}

func (c *Consumer) Listen(ctx context.Context, channel string, consumer msg.ReceiverFunc) error {
	c.readerCfg.Topic = channel
	reader := kafka.NewReader(c.readerCfg)

	defer func(reader *kafka.Reader) {
		err := reader.Close()
		if err != nil {
			c.logger.Error("error closing kafka-go reader: %v", err)
		}
	}(reader)

	for {
		err := c.receiveMessage(ctx, reader, consumer)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (c *Consumer) Close(ctx context.Context) error {
	c.logger.Trace("closing kafka consumer")
	return nil
}

func (c *Consumer) receiveMessage(ctx context.Context, reader *kafka.Reader, consumer msg.ReceiverFunc) error {
	m, err := reader.FetchMessage(ctx)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	var message msg.Message
	message, err = c.serializer.Deserialize(m)
	if err != nil {
		return err
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
			if ackErr := reader.CommitMessages(ctx, m); ackErr != nil {
				c.logger.Error("failed to acknowledging messages, err: %v", ackErr)
			}
		}
	case <-ctx.Done():
		c.logger.Trace("listener has closed the message")
	case <-wCtx.Done():
		c.logger.Warn("timeout waiting for message consumer to finish")
	}

	return nil
}

type ConsumerOption func(c *Consumer)

func WithConsumerDialer(dialer *kafka.Dialer) ConsumerOption {
	return func(c *Consumer) {
		c.readerCfg.Dialer = dialer
	}
}

func WithConsumerAckWait(d time.Duration) ConsumerOption {
	return func(c *Consumer) {
		c.ackWait = d
	}
}

func WithConsumerSerializer(serializer Serializer) ConsumerOption {
	return func(c *Consumer) {
		c.serializer = serializer
	}
}

func WithConsumerLogger(logger msg.Logger) ConsumerOption {
	return func(c *Consumer) {
		c.logger = logger
	}
}
