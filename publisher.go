package message_go

import (
	"context"
	"sync"
	"time"
)

type Publisher struct {
	producer Producer
	close    sync.Once
	logger   Logger
}

func NewPublisher(producer Producer, options ...PublisherOption) *Publisher {
	p := &Publisher{
		producer: producer,
		logger:   DefaultLogger,
	}

	for _, option := range options {
		option(p)
	}

	return p
}

func (p *Publisher) Publish(ctx context.Context, message Message) error {
	var err error
	var channel string

	channel, err = message.Headers().Get(MessageChannel)
	if err != nil {
		return err
	}

	message.Headers().Set(MessageDate, time.Now().Format(time.RFC3339))

	p.logger.Trace("publishing message, channel: %v, len: %v", channel, len(message.Payload()))

	err = p.producer.Send(ctx, channel, message)
	if err != nil {
		p.logger.Error("error publishing message: %v", err)
		return err
	}

	return nil
}

func (p *Publisher) Close(ctx context.Context) (err error) {
	defer p.logger.Trace("publisher stopped")
	p.close.Do(func() {
		err = p.producer.Close(ctx)
	})

	return err
}

type PublisherOption func(*Publisher)

func WithLogger(logger Logger) PublisherOption {
	return func(p *Publisher) {
		p.logger = logger
	}
}
