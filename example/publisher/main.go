package main

import (
	"context"
	msg "github.com/jinleibill/message-go"
	"github.com/jinleibill/message-go/kafka"
)

func main() {
	brokers := []string{"192.168.64.7:9092"}
	producer := kafka.NewProducer(brokers)
	publisher := msg.NewPublisher(producer)

	ctx := context.Background()
	defer func(publisher *msg.Publisher, ctx context.Context) {
		err := publisher.Close(ctx)
		if err != nil {
			msg.DefaultLogger.Error("关闭发布者失败: %v", err)
		}
	}(publisher, ctx)

	payload := []byte("hello world")
	headers := make(msg.Headers)
	headers.Set(msg.MessageChannel, "example.call")
	pMsg := msg.NewMessage(payload, msg.WithHeaders(headers))

	err := publisher.Publish(ctx, pMsg)
	if err != nil {
		panic(err)
	}

	msg.DefaultLogger.Debug("消息已发布")
}
