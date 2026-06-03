package metrics

// Flag names and shared metrics-server sentinels. These live here (rather than on
// a metrics manager) because they are consumed across the protocol — the smart
// router CLI flags, the Kafka/relay-server clients, the health metrics server, and
// the cache ecosystem all reference them, independently of any concrete manager.
const (
	MetricsListenFlagName         = "metrics-listen-address"
	RelayServerFlagName           = "relay-server-address"
	RelayKafkaFlagName            = "relay-kafka-address"
	RelayKafkaTopicFlagName       = "relay-kafka-topic"
	RelayKafkaUsernameFlagName    = "relay-kafka-username"
	RelayKafkaPasswordFlagName    = "relay-kafka-password"
	RelayKafkaMechanismFlagName   = "relay-kafka-mechanism"
	RelayKafkaTLSEnabledFlagName  = "relay-kafka-tls-enabled"
	RelayKafkaTLSInsecureFlagName = "relay-kafka-tls-insecure"
	// DisabledFlagOption disables a network-address-style flag (metrics server,
	// relay server, kafka). A manager/client given this value is a no-op.
	DisabledFlagOption = "disabled"
)

// ShowProviderEndpointInMetrics is bound to the --show-provider-address-in-metrics
// flag. The consumer metrics it once gated have been removed; the var is retained so
// the flag binding stays valid until the flag itself is retired.
var ShowProviderEndpointInMetrics = false
