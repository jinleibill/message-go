package main

import (
	"context"
	msg "github.com/jinleibill/message-go"
	"github.com/jinleibill/message-go/nats"
)

func main() {
	brokers := []string{"nats://127.0.0.1:4222"}
	msgProducer := nats.NewProducer(brokers)
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
