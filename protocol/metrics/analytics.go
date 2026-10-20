package metrics

import (
	"strings"
	"time"
	"unicode/utf8"
)

// A push on an open subscription is emitted as its own usage event under this method, charged at
// the flat delivery CU. It is not a request the client made, so the per-customer request counters
// skip it (SetRelayMetrics); the subscribe request itself is counted under its real method.
const (
	SubscriptionDeliveryMethod           = "subscription_delivery"
	DefaultSubscriptionDeliveryCU uint64 = 10
)

type RelayMetrics struct {
	RequestKind         string           // server-owned: internal, customer, notification
	Customer            *CustomerRequest // listener-owned lifecycle; nil for internal work and pushed notifications
	ProjectHash         string
	Timestamp           time.Time
	ChainID             string
	APIType             string
	Latency             int64
	Success             bool
	ComputeUnits        uint64
	Origin              string
	ApiMethod           string
	ProcessingTimestamp time.Time
	// Request classification fields — populated at relay call sites
	ProviderAddress string
	CacheHit        bool // true only when the response was served from the cache
	IsWrite         bool // stateful != 0; false means read
	IsArchive       bool
	IsDebugTrace    bool
	IsBatch         bool
	// Incident tracking fields — set during relay processing
	HedgeCount uint64 // number of hedge relays sent by the batch ticker for this request
}

func NewRelayAnalytics(projectHash, chainId, apiType string) *RelayMetrics {
	return &RelayMetrics{
		RequestKind: "internal",
		Timestamp:   time.Now(),
		ProjectHash: projectHash,
		ChainID:     chainId,
		APIType:     apiType,
	}
}

func (rm *RelayMetrics) SetProcessingTimestampBeforeRelay(timestamp time.Time) {
	if rm == nil {
		return
	}
	rm.ProcessingTimestamp = timestamp
}

func (rm *RelayMetrics) SetProcessingTimestampAfterRelay(timestamp time.Time) {
	if rm == nil {
		return
	}
	rm.ProcessingTimestamp = timestamp
}

// RelayUsageEvent is the wire format for raw per-relay usage events emitted as
// OTel logs (body="relay_usage") to the host-local collector. One event per
// relay, no consumer-side aggregation — the destination rolls up by
// (timestamp, project, chain, ...) at query time. JSON tags are kept short to
// minimize payload size at scale.
type RelayUsageEvent struct {
	Customer        *CustomerTerminal `json:"customer_request,omitempty"`
	RequestKind     string            `json:"request_kind"`
	TimestampNs     int64             `json:"ts"`
	ProjectHash     string            `json:"project"`
	ChainID         string            `json:"chain"`
	APIInterface    string            `json:"api_interface"`
	APIMethod       string            `json:"api_method,omitempty"`
	ComputeUnits    uint64            `json:"cu"`
	LatencyMs       int64             `json:"latency_ms"`
	Success         bool              `json:"success"`
	CacheHit        bool              `json:"cache_hit,omitempty"`
	IsWrite         bool              `json:"is_write,omitempty"`
	IsArchive       bool              `json:"is_archive,omitempty"`
	IsBatch         bool              `json:"is_batch,omitempty"`
	IsDebugTrace    bool              `json:"is_debug_trace,omitempty"`
	HedgeCount      uint64            `json:"hedge_count,omitempty"`
	ProviderAddress string            `json:"provider,omitempty"`
	Origin          string            `json:"origin,omitempty"`
}

func NewRelayUsageEvent(rm *RelayMetrics) RelayUsageEvent {
	event := RelayUsageEvent{
		TimestampNs:     rm.Timestamp.UnixNano(),
		ProjectHash:     validUTF8(rm.ProjectHash),
		ChainID:         rm.ChainID,
		APIInterface:    rm.APIType,
		APIMethod:       validUTF8(rm.ApiMethod),
		ComputeUnits:    rm.ComputeUnits,
		LatencyMs:       rm.Latency,
		Success:         rm.Success,
		CacheHit:        rm.CacheHit,
		IsWrite:         rm.IsWrite,
		IsArchive:       rm.IsArchive,
		IsBatch:         rm.IsBatch,
		IsDebugTrace:    rm.IsDebugTrace,
		HedgeCount:      rm.HedgeCount,
		ProviderAddress: rm.ProviderAddress,
		Origin:          validUTF8(rm.Origin),
	}
	event.RequestKind = rm.RequestKind
	if event.RequestKind == "" {
		event.RequestKind = "internal"
	}
	if rm.Customer != nil {
		snapshot := rm.Customer.Snapshot()
		snapshot.RequestID = validUTF8(snapshot.RequestID)
		snapshot.APIMethod = validUTF8(snapshot.APIMethod)
		event.Customer = &snapshot
		event.RequestKind = snapshot.RequestKind
		event.Success = snapshot.Outcome == "success"
		event.CacheHit = snapshot.CacheHit
		event.ProviderAddress = snapshot.Provider
		event.LatencyMs = snapshot.DurationMs
	}
	return event
}

// validUTF8 keeps a string that came with the request exportable. OTLP carries each
// attribute as a protobuf string, which must be valid UTF-8, and one invalid value
// fails the whole export batch. The customer label's cap does not apply here.
func validUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}
