package main

import (
	"context"
	msg "github.com/jinleibill/message-go"
	"github.com/jinleibill/message-go/kafka"
)

func main() {
	brokers := []string{"192.168.64.7:9092"}
	msgProducer := kafka.NewProducer(brokers)
	ctx := context.Background()
	defer msgProducer.Close(ctx)

	payload := []byte("hello world")
	headers := make(msg.Headers)
	headers.Set("source", "producer")
	pMsg := msg.NewMessage(payload, msg.WithKey("my-key"), msg.WithHeaders(headers))

	err := msgProducer.Send(ctx, "my-topic", pMsg)
	if err != nil {
		panic(err)
	}
}
