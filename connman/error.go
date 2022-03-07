package connman

import "errors"

// ErrCorrReplyChanNotExist indicates reply channel is closed or not in the correlator.
var ErrCorrReplyChanNotExist = errors.New("reply channel is not exists")

// ErrDialFailed indicates amqp dialing has failed.
var ErrDialFailed = errors.New("dialing amqp message broker failed")

// ErrChannelCreationFailed indicates amqp channel creation is failed.
var ErrChannelCreationFailed = errors.New("unable to create amqp channel")

// ErrAMQPRPCRequestFailed indicates request failed to reach the destination.
var ErrAMQPRPCRequestFailed = errors.New("request to destination failed")
