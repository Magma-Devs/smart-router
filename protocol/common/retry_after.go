package common

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// MaxRetryAfter is the longest delay an upstream can ask for. RFC 9110 puts no ceiling on
// the header, so an upstream — or a proxy in front of it — can name one measured in years,
// and a caller honouring it verbatim parks a provider for that long. Anything above this
// clamps to it, which also keeps delta-seconds well clear of int64 nanosecond overflow.
const MaxRetryAfter = time.Hour

const maxRetryAfterSeconds = int(MaxRetryAfter / time.Second)

// RateLimitedError carries what the upstream told us about when to come back.
//
// It wraps StatusCodeError429 rather than replacing it, so every existing
// errors.Is(err, StatusCodeError429) check keeps matching and only callers that want the
// duration need to know this type exists.
type RateLimitedError struct {
	// RetryAfter is what the upstream asked for, or 0 when it said nothing. Zero means
	// "no opinion", never "retry immediately" — callers fall back to their own backoff.
	RetryAfter time.Duration

	// cause is the rate-limit error being enriched. Whatever context was already wrapped
	// around the sentinel stays reachable; nil means the bare sentinel.
	cause error
}

// underlying is what this error stands in for: the enriched error, or the sentinel when it
// was built directly from a duration.
func (e *RateLimitedError) underlying() error {
	if e.cause != nil {
		return e.cause
	}
	return StatusCodeError429
}

func (e *RateLimitedError) Error() string {
	if e.RetryAfter > 0 {
		return e.underlying().Error() + ", retry after " + e.RetryAfter.String()
	}
	return e.underlying().Error()
}

func (e *RateLimitedError) Unwrap() error { return e.underlying() }

// RetryAfterFrom extracts an upstream-requested delay from an error chain. ok is false when
// the error is not a rate-limit, or is one that carried no usable Retry-After.
func RetryAfterFrom(err error) (d time.Duration, ok bool) {
	var rle *RateLimitedError
	if errors.As(err, &rle) && rle.RetryAfter > 0 {
		return rle.RetryAfter, true
	}
	return 0, false
}

// ParseRetryAfter reads the header in either wire form RFC 9110 permits: delta-seconds
// ("120") or an HTTP-date ("Wed, 21 Oct 2015 07:28:00 GMT"). Anything unparseable, zero,
// negative, or already in the past is treated as absent — a malformed header must leave the
// caller on its own backoff, not push it to retry immediately. A delay longer than
// MaxRetryAfter clamps to it.
func ParseRetryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	if h == nil {
		return 0, false
	}
	raw := h.Get("Retry-After")
	if raw == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(raw); err == nil {
		if secs <= 0 {
			return 0, false
		}
		if secs > maxRetryAfterSeconds {
			return MaxRetryAfter, true
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(raw); err == nil {
		if d := t.Sub(retryAfterReferenceTime(h, now)); d > 0 {
			if d > MaxRetryAfter {
				return MaxRetryAfter, true
			}
			return d, true
		}
	}
	return 0, false
}

// retryAfterReferenceTime picks the clock an HTTP-date is measured against. The date is
// absolute, so a skewed upstream clock either inflates the wait or backdates the header into
// "already past"; its own Date header states the same clock that wrote the deadline, which
// cancels the skew. Local time is the fallback when it sends none.
func retryAfterReferenceTime(h http.Header, now time.Time) time.Time {
	if date, err := http.ParseTime(h.Get("Date")); err == nil {
		return date
	}
	return now
}

// RateLimited types err as an upstream rate refusal carrying retryAfter, for transports
// where WithRetryAfter's HTTP-header form does not apply (gRPC metadata, WS handshakes).
// The sentinel lands in the chain, so errors.Is(result, StatusCodeError429) holds, and
// RetryAfterFrom returns retryAfter when positive (clamped to MaxRetryAfter).
func RateLimited(err error, retryAfter time.Duration) error {
	cause := StatusCodeError429
	if err != nil {
		cause = fmt.Errorf("%w: %w", StatusCodeError429, err)
	}
	if retryAfter <= 0 {
		return cause
	}
	if retryAfter > MaxRetryAfter {
		retryAfter = MaxRetryAfter
	}
	return &RateLimitedError{RetryAfter: retryAfter, cause: cause}
}

// RateLimitFromGRPC reports whether a gRPC failure is the upstream refusing us for rate,
// and the wait it asked for (0 when it said nothing usable). md is the response metadata
// (gRPC keys are lowercase).
//
// gRPC carries no HTTP status, so the decision needs corroboration:
//   - a known rate-limit text in the status message always counts — including the "429
//     (Too Many Requests)" a vendor's HTTP edge leaves inside codes.Unavailable;
//   - RESOURCE_EXHAUSTED (code 8) counts only when the metadata carries a retry delay.
//     Alone it never suffices: grpc-go mints the same code for an oversized message.
func RateLimitFromGRPC(code uint32, message string, md map[string][]string) (time.Duration, bool) {
	retryAfter := grpcRetryDelayFromMD(md)
	msg := strings.ToLower(message)
	for _, sig := range []string{"too many requests", "rate limit", "ratelimit", "enhance_your_calm"} {
		if strings.Contains(msg, sig) {
			return retryAfter, true
		}
	}
	const grpcResourceExhausted = 8
	if code == grpcResourceExhausted && retryAfter > 0 {
		return retryAfter, true
	}
	return 0, false
}

// grpcRetryDelayFromMD reads the delay an upstream attached to a gRPC failure: the
// grpc-retry-pushback-ms convention, or a retry-after header a fronting HTTP proxy left
// in. Keys are matched lowercase — http.Header canonicalization does not apply to gRPC
// metadata. Clamped to MaxRetryAfter like every other capture.
func grpcRetryDelayFromMD(md map[string][]string) time.Duration {
	first := func(key string) string {
		if vs := md[key]; len(vs) > 0 {
			return vs[0]
		}
		return ""
	}
	if raw := first("grpc-retry-pushback-ms"); raw != "" {
		if ms, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && ms > 0 {
			d := time.Duration(ms) * time.Millisecond
			if d > MaxRetryAfter {
				return MaxRetryAfter
			}
			return d
		}
	}
	if raw := first("retry-after"); raw != "" {
		h := http.Header{}
		h.Set("Retry-After", raw)
		if d, ok := ParseRetryAfter(h, time.Now()); ok {
			return d
		}
	}
	return 0
}

// WithRetryAfter enriches a rate-limit error with the upstream's Retry-After, if it sent one.
//
// Any other error passes through untouched, so call sites can apply it unconditionally to
// whatever ValidateStatusCodes returned without first checking which error they have.
func WithRetryAfter(err error, h http.Header, now time.Time) error {
	if err == nil || !errors.Is(err, StatusCodeError429) {
		return err
	}
	d, ok := ParseRetryAfter(h, now)
	if !ok {
		return err
	}
	return &RateLimitedError{RetryAfter: d, cause: err}
}
