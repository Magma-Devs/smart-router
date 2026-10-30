package relaypolicy

// PolicyConfig configures the policy engine. Consumer and SmartRouter may use different values.
type PolicyConfig struct {
	MaxRetries              int  // hard ceiling on retry attempts
	RelayRetryLimit         int  // error tolerance (total errors before giving up)
	DisableBatchRetry       bool // whether batch requests can be retried
	EnableCircuitBreaker    bool // provider exhaustion detection (SmartRouter only)
	CircuitBreakerThreshold int  // consecutive pairing errors before tripping
	SendRelayAttempts       int  // consecutive batch errors before giving up
}
