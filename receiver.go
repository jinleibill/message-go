package message_go

import "context"

type Receiver interface {
	ReceiveMessage(ctx context.Context, message Message) error
}

var _ Receiver = (*ReceiverFunc)(nil)

type ReceiverFunc func(ctx context.Context, message Message) error

func (f ReceiverFunc) ReceiveMessage(ctx context.Context, message Message) error {
	return f(ctx, message)
}
