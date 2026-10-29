package relaycore

import (
	"errors"
	"net/http"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// transportFailureCodes are the registry codes that mean "no answer from the upstream": the
// router may speak for itself there. The value says whether the failure is a timeout, which is a
// 504; every other one is a 502 (MAG-4218, MAG-4481).
var transportFailureCodes = map[uint32]bool{
	1001: true,  // connection timeout
	1002: false, // connection refused
	1003: false, // dns failure
	1004: false, // tls mismatch
	1005: false, // connection reset
	1006: false, // connection closed
	1007: true,  // context deadline
	1009: false, // network or host unreachable
}

// clientFacingFromProtocolError decides what the caller should read for a failed relay. Two
// shapes qualify: the upstream answered with an error status (forward status and body as is), or
// the transport failed before any answer (502, or 504 for a timeout, and the leaf cause). Any other
// protocol error keeps today's reply. The classification travels as metadata only.
func clientFacingFromProtocolError(err error, lavaErr *common.LavaError) *common.ClientFacingError {
	if err == nil {
		return nil
	}
	cf := &common.ClientFacingError{}
	if lavaErr != nil {
		cf.RouterErrorCode = lavaErr.Code
		cf.RouterErrorName = lavaErr.Name
	}
	var httpErr *lavasession.HTTPStatusError
	if errors.As(err, &httpErr) && httpErr.StatusCode >= 400 {
		cf.UpstreamStatus = httpErr.StatusCode
		cf.UpstreamStatusText = httpErr.Status
		cf.UpstreamBody = httpErr.Body
		cf.UpstreamContentType = httpErr.ContentType
		return cf
	}
	if lavaErr != nil {
		if timedOut, isTransport := transportFailureCodes[lavaErr.Code]; isTransport {
			cf.Cause = leafCause(err)
			cf.TimedOut = timedOut
			return cf
		}
	}
	return nil
}

// NoAnswerStatus is the status the router answers with when no upstream answered: 504 for a
// timeout, 502 otherwise.
func NoAnswerStatus(cf *common.ClientFacingError) int {
	if cf != nil && cf.TimedOut {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

// replyProtocolError chooses the error the client's reply is built from (MAG-4218). An upstream's
// answer beats any other error, a fault beats a throttle (a 503 next to two 429s is the 503), and
// a transport failure is used only when every attempt failed in transport. Among transport
// failures a timeout wins, so a refused connection next to a timeout is always the 504, never a
// 502 on some runs: the majority pick breaks a tie at random (MAG-4481). Within a rank the
// majority pick stands; a higher rank takes the first such error.
func replyProtocolError(picked RelayError, all []RelayError) RelayError {
	best, bestRank := picked, replyRank(picked)
	for _, candidate := range all {
		if rank := replyRank(candidate); rank > bestRank {
			best, bestRank = candidate, rank
		}
	}
	return best
}

func replyRank(relayError RelayError) int {
	cf := clientFacingFromProtocolError(relayError.Err, relayError.LavaError)
	switch {
	case cf == nil:
		return 2 // today's reply
	case cf.UpstreamStatus == http.StatusTooManyRequests:
		// The upstream answered, but only to say it is throttling us. Before MAG-4481 a 429
		// carried no body and ranked as today's reply, so another upstream's fault won; keeping
		// the body must not change that.
		return 3
	case cf.UpstreamStatus != 0:
		return 4 // the upstream answered
	case cf.TimedOut:
		return 1 // no upstream answered in time: 504
	default:
		return 0 // no upstream answered: 502
	}
}

// leafCause returns the innermost error's text, skipping the registry wrapper's own description.
func leafCause(err error) string {
	var wrapped *common.LavaWrappedError
	if errors.As(err, &wrapped) {
		if cause := wrapped.Unwrap(); cause != nil {
			if _, isRegistry := cause.(*common.LavaError); !isRegistry {
				return cause.Error()
			}
		}
	}
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err.Error()
		}
		err = next
	}
}
