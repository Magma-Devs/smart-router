package common

// ClientFacingError carries, to the listener, what the caller should read when a relay ends in a
// protocol error (MAG-4218). Internals (classification, retry, health, QoS, counters)
// are untouched: this is read only when the reply is built.
type ClientFacingError struct {
	UpstreamStatus     int    // 0 when the upstream never answered
	UpstreamStatusText string // the status line, e.g. "503 Service Unavailable"
	UpstreamBody       []byte // the upstream's body, unmodified
	Cause              string // leaf cause text for a transport failure (no upstream answer)
	RouterErrorCode    uint32 // the registry classification, as metadata
	RouterErrorName    string
}

// GetClientFacing is nil-safe, like the other RelayResult getters.
func (rr *RelayResult) GetClientFacing() *ClientFacingError {
	if rr == nil {
		return nil
	}
	return rr.ClientFacing
}
