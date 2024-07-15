package main

import (
	"context"
	"fmt"
	msg "github.com/jinleibill/message-go"
	"github.com/jinleibill/message-go/kafka"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	brokers := []string{"192.168.64.7:9092"}
	consumer := kafka.NewConsumer(brokers, "example")
	subscriber := msg.NewSubscriber(consumer)
	subscriber.Use(MessageProcessTime())

	subscriber.Subscribe("example.call", NewSay())
	subscriber.Subscribe("example.call", NewSay2())
	subscriber.Subscribe("example.call2", NewSay2())

	ctx, cancel := context.WithCancel(context.Background())
	group, gCtx := errgroup.WithContext(ctx)

	group.Go(func() error {
		defer msg.DefaultLogger.Debug("订阅者已经退出")
		return subscriber.Start(ctx)
	})

	group.Go(func() error {
		<-gCtx.Done()
		msg.DefaultLogger.Debug("开始关闭订阅者")
		sCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := subscriber.Stop(sCtx); err != nil {
				msg.DefaultLogger.Error("订阅者超时关闭失败")
			}
		}()

		done := make(chan struct{})
		go func() {
			defer close(done)
			wg.Wait()
		}()

		select {
		case <-done:
		case <-sCtx.Done():
			msg.DefaultLogger.Warn("超时关闭订阅者")
		}

		return nil
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh

	cancel()

	if err := group.Wait(); err != nil {
		msg.DefaultLogger.Error("关闭订阅者错误")
	}
}

func MessageProcessTime() func(msg.Receiver) msg.Receiver {
	return func(next msg.Receiver) msg.Receiver {
		return msg.ReceiverFunc(func(ctx context.Context, message msg.Message) error {
			startTime := time.Now()
			err := next.ReceiveMessage(ctx, message)
			msg.DefaultLogger.Info("消息处理时间: %v ms", time.Since(startTime).Microseconds())

			return err
		})
	}
}

type Say struct{}

func NewSay() *Say {
	return &Say{}
}

func (s Say) ReceiveMessage(ctx context.Context, message msg.Message) error {
	fmt.Println("say 接收器: 消息接收到了", message.Headers(), string(message.Payload()))
	return nil
}

type Say2 struct{}

func NewSay2() *Say2 {
	return &Say2{}
}

func (s Say2) ReceiveMessage(ctx context.Context, message msg.Message) error {
	fmt.Println("say2 接收器: 消息接收到了", message.Headers(), string(message.Payload()))
	return nil
}
