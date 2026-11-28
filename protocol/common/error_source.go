package common

import "context"

// ErrorSource says who sent the attempt a counted error belongs to: the `source` label of
// smartrouter_errors_total (MAG-5598; PRD §9, rulebook R-G, Q183). Only ErrorSourceClient is a
// client's own request: the SLA queries and the dashboard's error pivots read
// source=~"client|", where the empty value matches a router that doesn't emit the label yet.
type ErrorSource string

const (
	// ErrorSourceClient is a request a client sent.
	ErrorSourceClient ErrorSource = "client"
	// ErrorSourceProbe is the router's own health traffic once an endpoint serves: the probe
	// loop and the relays monitor's crafted relays.
	ErrorSourceProbe ErrorSource = "probe"
	// ErrorSourceBoot is the checks and first crafted relays an endpoint runs before it serves.
	ErrorSourceBoot ErrorSource = "boot"
	// ErrorSourceReverify is a provider verified again: the epoch pass and the retry loops.
	ErrorSourceReverify ErrorSource = "reverify"
	// ErrorSourcePoll is the endpoint pollers the chain trackers read heads through.
	ErrorSourcePoll ErrorSource = "poll"
	// ErrorSourceSubscription is a subscribe the node refused, or an error frame the node sent on
	// an open subscription (PRD §5.7).
	ErrorSourceSubscription ErrorSource = "subscription"
)

type errorSourceKey struct{}

// WithErrorSource tags ctx: the errors of attempts made under it are counted with source.
func WithErrorSource(ctx context.Context, source ErrorSource) context.Context {
	return context.WithValue(ctx, errorSourceKey{}, source)
}

// WithDefaultErrorSource tags ctx with source unless a caller already tagged it, so a path that
// runs on behalf of another keeps that one's source (a poll the probe loop asks for is a probe).
func WithDefaultErrorSource(ctx context.Context, source ErrorSource) context.Context {
	if _, ok := ctx.Value(errorSourceKey{}).(ErrorSource); ok {
		return ctx
	}
	return WithErrorSource(ctx, source)
}

// ErrorSourceOf is the source ctx was tagged with. An untagged ctx is a client's: the router's own
// paths tag theirs where they start, so a path that forgets to errs toward counting its errors in
// the SLA's number, never toward hiding a client's.
func ErrorSourceOf(ctx context.Context) ErrorSource {
	if ctx != nil {
		if source, ok := ctx.Value(errorSourceKey{}).(ErrorSource); ok && source != "" {
			return source
		}
	}
	return ErrorSourceClient
}

// ErrorOrigin is what smartrouter_errors_total records about where a counted error came from, beside
// its name and chain: the interface of the attempt and who sent it.
type ErrorOrigin struct {
	// APIInterface is the spec's interface name (jsonrpc, tendermintrpc, rest, grpc, graphql), the
	// same values as smartrouter_node_errors_total's apiInterface.
	APIInterface string
	Source       ErrorSource
}

// ErrorOriginOf is the origin of an error on apiInterface, from the source ctx was tagged with.
func ErrorOriginOf(ctx context.Context, apiInterface string) ErrorOrigin {
	return ErrorOrigin{APIInterface: apiInterface, Source: ErrorSourceOf(ctx)}
}
