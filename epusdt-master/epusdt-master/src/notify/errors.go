package notify

import "errors"

var (
	ErrNoSender    = errors.New("no sender registered for channel type")
	ErrEmptyConfig = errors.New("channel config is empty")
)
