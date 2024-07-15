package message_go

import "context"

type Producer interface {
	Send(ctx context.Context, channel string, message Message) error
	Close(ctx context.Context) error
}
