package utils

import (
	"fmt"
	"regexp"
	"sync/atomic"
)

var DebugPaymentE2E = "" // using "debug_payment_e2e" build option, this string will be "debug_payment_e2e"

// logUnsafePayloads gates whether RPC request/response payloads are logged
// without scrubbing. It defaults to false: API keys and IP addresses embedded
// in bodies, params, and URLs are replaced with a placeholder so they do not
// leak into container logs, log collectors, or support bundles. Operators set
// it to true via the --log-unsafe-payloads flag for local debugging only.
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

const redactedSecretValue = "[REDACTED]"

// Scrubbing patterns. Deliberately narrow: we only want to strip API keys and
// IP addresses, not mangle the rest of the payload. Over-broad patterns would
// redact legitimate hex (block hashes, addresses) and make logs useless.
var (
	// API keys carried in URL paths/queries: ?key=… / ?api_key=… / ?apikey=… /
	// ?token=… / ?access_token=… / ?auth=… — capture the param name, redact the
	// value up to the next & or whitespace or quote.
	urlKeyQueryRegex = regexp.MustCompile(`(?i)([?&](?:api[_-]?key|key|token|access[_-]?token|auth)=)[^&\s"']+`)

	// API keys carried as a provider URL path segment, e.g.
	// /v2/<32+ char token>, /v3/<token>, /ethereum/<token>. Match a long
	// (>=20 char) opaque segment that looks like a credential, not a normal path.
	urlKeyPathRegex = regexp.MustCompile(`(/(?:v\d+|rpc|gateway|[a-z]+))/([A-Za-z0-9_\-]{20,})`)

	// IPv4 addresses.
	ipv4Regex = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)

	// IPv6 addresses (compressed or full). Conservative: requires at least two
	// colon-separated hextet groups so we don't match ordinary "a:b" text.
	ipv6Regex = regexp.MustCompile(`\b(?:[0-9A-Fa-f]{1,4}:){2,7}[0-9A-Fa-f]{1,4}\b`)
)

// ScrubSecrets removes API keys and IP addresses from s, leaving the rest of
// the content readable. Used by the payload log helpers when unsafe logging is
// off. Order matters: URL key patterns run before the IP patterns so a token
// that happens to contain dotted digits is handled as a key first.
func ScrubSecrets(s string) string {
	if s == "" {
		return s
	}
	s = urlKeyQueryRegex.ReplaceAllString(s, "${1}"+redactedSecretValue)
	s = urlKeyPathRegex.ReplaceAllString(s, "${1}/"+redactedSecretValue)
	s = ipv4Regex.ReplaceAllString(s, redactedSecretValue)
	s = ipv6Regex.ReplaceAllString(s, redactedSecretValue)
	return s
}

// RedactPayload returns the payload verbatim when unsafe payload logging is
// enabled, otherwise a copy with API keys and IP addresses scrubbed out. The
// body stays readable for debugging; only secrets are removed. Use it on every
// log attribute that carries an RPC request/response body, URL, or param.
func RedactPayload(payload string) string {
	if LogUnsafePayloadsEnabled() {
		return payload
	}
	return ScrubSecrets(payload)
}

// RedactPayloadAny is the any-typed variant for log attributes that carry
// already-parsed params/values (interface{}). When unsafe logging is on it
// returns the value unchanged; otherwise it renders the value to a string and
// scrubs API keys and IPs out of it.
func RedactPayloadAny(payload interface{}) interface{} {
	if LogUnsafePayloadsEnabled() {
		return payload
	}
	if payload == nil {
		return nil
	}
	return ScrubSecrets(fmt.Sprintf("%v", payload))
}
