package utils

import (
	"fmt"
	"sync/atomic"
)

var DebugPaymentE2E = "" // using "debug_payment_e2e" build option, this string will be "debug_payment_e2e"

// logUnsafePayloads gates whether raw RPC request/response payloads (bodies)
// are written to logs verbatim. It defaults to false: payloads are redacted to
// a length-only placeholder so wallet addresses, transaction data, dapp
// identifiers, and credentials embedded in bodies do not leak into container
// logs, log collectors, or support bundles. Operators set it to true via the
// --log-unsafe-payloads flag for local debugging only.
//
// Stored as an atomic so it can be read from the hot relay path on every
// request without a lock; it is written exactly once at startup.
var logUnsafePayloads atomic.Bool

// SetLogUnsafePayloads enables or disables verbatim payload logging. Call once
// at startup from the --log-unsafe-payloads flag.
func SetLogUnsafePayloads(enabled bool) {
	logUnsafePayloads.Store(enabled)
}

// LogUnsafePayloadsEnabled reports whether verbatim payload logging is on.
func LogUnsafePayloadsEnabled() bool {
	return logUnsafePayloads.Load()
}

const redactedPayloadValue = "[REDACTED]"

// RedactPayload returns the payload as-is when unsafe payload logging is
// enabled, otherwise a length-only placeholder. Use it on every log attribute
// that carries an RPC request or response body. The byte length is preserved
// in the redacted form so operators can still see traffic shape and spot
// empty-vs-non-empty bodies without exposing contents.
func RedactPayload(payload string) string {
	if LogUnsafePayloadsEnabled() {
		return payload
	}
	if payload == "" {
		return ""
	}
	return fmt.Sprintf("%s(%d bytes)", redactedPayloadValue, len(payload))
}
