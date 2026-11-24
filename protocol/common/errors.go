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

	// UnservedGRPCCallError marks a gRPC call the router refuses itself because it does not serve
	// the call's shape: a client stream, a two-way stream, or a server stream the chain spec does
	// not name as a subscription (MAG-5133). Every provider would be asked the same call, so it is
	// never retried, and no provider is blamed for it. Its text is the whole sentence the gRPC
	// listener gives the caller, with status UNIMPLEMENTED.
	UnservedGRPCCallError = errors.New("The router does not serve this gRPC call type: it serves single-reply calls, and the server streams the chain spec names as subscriptions")
)
