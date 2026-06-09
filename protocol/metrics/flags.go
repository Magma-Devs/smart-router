package metrics

// Flag names and shared metrics-server sentinels. These live here (rather than on
// a metrics manager) because they are consumed across the protocol — the smart
// router CLI flags, the usage-telemetry sink, the health metrics server, and the
// cache ecosystem all reference them, independently of any concrete manager.
const (
	MetricsListenFlagName = "metrics-listen-address"

	// Usage telemetry (OTel) — disabled by default. When off the relay path
	// pays one virtual call per relay and nothing else; no SDK setup, no
	// goroutines, no batching. Flip on when you have a collector ready.
	UsageOTelEnabledFlagName       = "usage-otel-enabled"
	UsageOTelEndpointFlagName      = "usage-otel-endpoint"
	UsageOTelInsecureFlagName      = "usage-otel-insecure"
	UsageOTelQueueSizeFlagName     = "usage-otel-queue-size"
	UsageOTelBatchSizeFlagName     = "usage-otel-batch-size"
	UsageOTelFlushIntervalFlagName = "usage-otel-flush-interval"
	UsageOTelExportTimeoutFlagName = "usage-otel-export-timeout"
	UsageOTelServiceNameFlagName   = "usage-otel-service-name"
	UsageOTelInstanceIDFlagName    = "usage-otel-service-instance-id"

	// DisabledFlagOption disables a network-address-style flag (e.g. the
	// metrics server). A manager/client given this value is a no-op.
	DisabledFlagOption = "disabled"
)
