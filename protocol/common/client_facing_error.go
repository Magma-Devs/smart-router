package common

import "time"

// ClientFacingError carries, to the listener, what the caller should read when a relay ends in a
// protocol error (MAG-4218, MAG-4481). The router is a proxy: when an upstream answered, its status
// and body go back as is; when none did, the router says so in one sentence. Internals
// (classification, retry, health, QoS, counters) are untouched: this is read only when the reply is
// built.
type ClientFacingError struct {
	UpstreamStatus      int    // 0 when the upstream never answered
	UpstreamStatusText  string // the status line, e.g. "503 Service Unavailable"
	UpstreamBody        []byte // the upstream's body, unmodified
	UpstreamContentType string // the upstream's Content-Type, empty when it sent none
	Cause               string // leaf cause text for a transport failure (no upstream answer)
	// TimedOut marks a no-answer that is a timeout: the reply is 504 and names Timeout.
	TimedOut bool
	// Timeout is the request's processing timeout, filled by the router once the relay has ended.
	Timeout         time.Duration
	RouterErrorCode uint32 // the registry classification, as metadata
	RouterErrorName string
}

// NoAnswerMessage is the router's one sentence when no upstream answered.
func (cf *ClientFacingError) NoAnswerMessage() string {
	if cf.TimedOut {
		if cf.Timeout > 0 {
			return "no answer from upstream within " + cf.Timeout.String()
		}
		return "no answer from upstream within the request's timeout"
	}
	return "no answer from upstream: " + cf.Cause
}

// GetClientFacing is nil-safe, like the other RelayResult getters.
func (rr *RelayResult) GetClientFacing() *ClientFacingError {
	if rr == nil {
		return nil
	}
	return rr.ClientFacing
}
