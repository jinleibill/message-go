package main

import (
	"context"
	"fmt"
	msg "github.com/jinleibill/message-go"
	"github.com/jinleibill/message-go/kafka"
	"time"
)

func main() {
	brokers := []string{"192.168.64.7:9092"}
	msgConsumer := kafka.NewConsumer(brokers, "my-group")
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
