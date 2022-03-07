package alacli

import "errors"

// ErrPublishFailed is a general error for failing to publish a message.
var ErrPublishFailed = errors.New("publishing a message failed")

// ErrCreateChanFailed indicates channel creation has failed to conduct.
var ErrCreateChanFailed = errors.New("creating channel has failed")
