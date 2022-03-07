package correlator

import "errors"

// ErrCorrReplyChanNotExist indicates reply channel is closed or not in the correlator.
var ErrCorrReplyChanNotExist = errors.New("reply channel is not exists")
