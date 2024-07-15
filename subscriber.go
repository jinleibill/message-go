package message_go

import (
	"context"
	"golang.org/x/sync/errgroup"
	"sync"
)

type Subscriber struct {
	consumer    Consumer
	middlewares []func(Receiver) Receiver
	receivers   map[string][]Receiver
	stopping    chan struct{}
	wg          sync.WaitGroup
	close       sync.Once
	logger      Logger
}

func NewSubscriber(consumer Consumer, options ...SubscriberOption) *Subscriber {
	sub := &Subscriber{
		consumer:  consumer,
		receivers: make(map[string][]Receiver),
		stopping:  make(chan struct{}),
		logger:    DefaultLogger,
	}

	for _, option := range options {
		option(sub)
	}

	return sub
}

func (sub *Subscriber) Use(mws ...func(Receiver) Receiver) {
	if len(sub.receivers) > 0 {
		panic("middleware must be added before any subscriptions are made")
	}

	sub.middlewares = append(sub.middlewares, mws...)
}

func (sub *Subscriber) chain(receiver Receiver) Receiver {
	if len(sub.middlewares) == 0 {
		return receiver
	}

	r := sub.middlewares[len(sub.middlewares)-1](receiver)
	for i := len(sub.middlewares) - 2; i >= 0; i-- {
		r = sub.middlewares[i](r)
	}

	return r
}

func (sub *Subscriber) Subscribe(channel string, receiver Receiver) {
	if _, exists := sub.receivers[channel]; !exists {
		sub.receivers[channel] = []Receiver{}
	}

	sub.logger.Trace("subscribed channel: %s", channel)
	sub.receivers[channel] = append(sub.receivers[channel], sub.chain(receiver))
}

func (sub *Subscriber) Start(ctx context.Context) error {
	cCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, gCtx := errgroup.WithContext(cCtx)

	group.Go(func() error {
		select {
		case <-sub.stopping:
			cancel()
		case <-gCtx.Done():
		}

		return nil
	})

	for c, r := range sub.receivers {
		channel := c
		receivers := r

		sub.wg.Add(1)

		group.Go(func() error {
			defer sub.wg.Done()

			receiverFunc := func(mCtx context.Context, message Message) error {
				sub.logger.Trace("received message, key: %v, len: %v", message.Key(), message.Payload())

				rGroup, rCtx := errgroup.WithContext(mCtx)
				for _, r2 := range receivers {
					receiver := r2
					rGroup.Go(func() error {
						return receiver.ReceiveMessage(rCtx, message)
					})
				}

				return rGroup.Wait()
			}

			err := sub.consumer.Listen(gCtx, channel, receiverFunc)
			if err != nil {
				sub.logger.Error("consumer stopped and returned an error: %v", err)
				return err
			}

			return nil
		})
	}

	return group.Wait()
}

func (sub *Subscriber) Stop(ctx context.Context) (err error) {
	sub.close.Do(func() {
		close(sub.stopping)

		done := make(chan struct{})
		go func() {
			err = sub.consumer.Close(ctx)
			sub.wg.Wait()
			close(done)
		}()

		select {
		case <-ctx.Done():
			sub.logger.Warn("timed out waiting for all receivers to close")
		case <-done:
			sub.logger.Trace("all receivers are done")
		}
	})

	return
}

type SubscriberOption func(*Subscriber)

func WithSubscriberLogger(logger Logger) SubscriberOption {
	return func(sub *Subscriber) {
		sub.logger = logger
	}
}
