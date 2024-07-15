package kafka

import (
	"context"
	msg "github.com/jinleibill/message-go"
	"github.com/segmentio/kafka-go"
)

var _ msg.Producer = (*Producer)(nil)

type Producer struct {
	writer     *kafka.Writer
	serializer Serializer
	logger     msg.Logger
}

func NewProducer(brokers []string, options ...ProducerOption) *Producer {
	p := &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
		},
		serializer: DefaultSerializer,
		logger:     msg.DefaultLogger,
	}

	for _, option := range options {
		option(p)
	}

	return p
}

func (p *Producer) Send(ctx context.Context, channel string, message msg.Message) error {
	kafkaMsg, err := p.serializer.Serialize(message)
	if err != nil {
		p.logger.Error("failed to marshal message, err: %v", err)
		return err
	}

	kafkaMsg.Topic = channel

	return p.writer.WriteMessages(ctx, kafkaMsg)
}

func (p *Producer) Close(ctx context.Context) error {
	p.logger.Trace("closing kafka producer")
	return p.writer.Close()
}

type ProducerOption func(*Producer)

func WithProducerBalancer(balancer kafka.Balancer) ProducerOption {
	return func(p *Producer) {
		p.writer.Balancer = balancer
	}
}

func WithProducerTransport(transport *kafka.Transport) ProducerOption {
	return func(p *Producer) {
		p.writer.Transport = transport
	}
}

func WithProducerCompression(compression kafka.Compression) ProducerOption {
	return func(p *Producer) {
		p.writer.Compression = compression
	}
}

func WithProducerSerializer(serializer Serializer) ProducerOption {
	return func(p *Producer) {
		p.serializer = serializer
	}
}

func WithProducerLogger(logger msg.Logger) ProducerOption {
	return func(p *Producer) {
		p.logger = logger
	}
}
