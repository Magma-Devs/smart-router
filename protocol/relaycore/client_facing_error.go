package relaycore

import (
	"errors"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// transportFailureCodes are the registry codes that mean "no answer from the upstream": the
// router may speak for itself there, and 502 is the honest status (MAG-4218).
var transportFailureCodes = map[uint32]bool{
	1001: true, // connection timeout
	1002: true, // connection refused
	1003: true, // dns failure
	1004: true, // tls mismatch
	1005: true, // connection reset
	1006: true, // connection closed
	1007: true, // context deadline
	1043: true, // no response within the protocol cutoff
}

// clientFacingFromProtocolError decides what the caller should read for a failed relay. Two
// shapes qualify: the upstream answered with an error status (forward status and body), or the
// transport failed before any answer (502 and the leaf cause). Any other protocol error keeps
// today's reply. The classification travels as metadata only.
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
		return cf
	}
	if lavaErr != nil && transportFailureCodes[lavaErr.Code] {
		cf.Cause = leafCause(err)
		return cf
	}
	return nil
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
