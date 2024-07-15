package message_go

import "context"

type Consumer interface {
	Listen(ctx context.Context, channel string, consumer ReceiverFunc) error
	Close(ctx context.Context) error
}
