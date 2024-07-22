package main

import (
	"context"
	"fmt"
	msg "github.com/jinleibill/message-go"
	"github.com/jinleibill/message-go/nats"
	"time"
)

func main() {
	brokers := []string{"nats://127.0.0.1:4222"}
	msgConsumer := nats.NewConsumer(brokers)
	ctx := context.Background()
	defer msgConsumer.Close(ctx)

	err := msgConsumer.Listen(ctx, "my-topic", func(ctx context.Context, message msg.Message) error {
		fmt.Println(string(message.Payload()), message.Key(), message.Headers())

		time.Sleep(15 * time.Second)
		return nil
	})
	if err != nil {
		panic(err)
	}
}
