package common

import (
	"errors"
)

var (
	ContextDeadlineExceededError = errors.New("context deadline exceeded")
	StatusCodeError504           = errors.New("Disallowed status code error (504)")
	StatusCodeError429           = errors.New("Disallowed status code error (429)")
	StatusCodeErrorStrict        = errors.New("Disallowed status code error (800)")
	APINotSupportedError         = errors.New("api not supported")
	SubscriptionNotFoundError    = errors.New("subscription not found")
)
