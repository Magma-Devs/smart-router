package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// Verify the real OTLP/HTTP encoder, not just an in-process exporter: the final
// snapshot and caller strings must reach a collector without truncation or an
// invalid protobuf string rejecting the entire batch.
func TestCustomerSnapshotReachesOTLPHTTP(t *testing.T) {
	received := make(chan []byte, 4)
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- body
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer collector.Close()
	sink := NewOTelUsageSink(OTelUsageSinkConfig{Endpoint: strings.TrimPrefix(collector.URL, "http://"), Insecure: true, BatchSize: 1, FlushInterval: 10 * time.Millisecond})
	require.NotNil(t, sink)
	defer sink.Close()
	logger, err := NewRPCConsumerLogs(&customerRecorder{}, sink, nil)
	require.NoError(t, err)
	data := NewRelayAnalytics("caller\xff", "ETH1", "jsonrpc")
	data.ApiMethod = "eth_getBalance"
	ctx := logger.BeginCustomerRequest(context.Background(), data)
	MarkCustomerReroute(ctx, "node-a", "primary-pool-unavailable", []string{"node-b"})
	data.CaptureCustomerResult(providerResult("node-b"), "Success", []CustomerAttempt{{Provider: "node-a", Outcome: "failure", ErrorName: "PROTOCOL_CONNECTION_REFUSED"}}, nil, false)
	logger.AddMetricForHttp(data, nil, nil)
	var wire []byte
	select {
	case wire = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("collector did not receive the customer terminal snapshot")
	}
	var request collectorlogs.ExportLogsServiceRequest
	require.NoError(t, proto.Unmarshal(wire, &request))
	require.Len(t, request.ResourceLogs, 1)
	require.Len(t, request.ResourceLogs[0].ScopeLogs, 1)
	records := request.ResourceLogs[0].ScopeLogs[0].LogRecords
	require.Len(t, records, 1)
	attrs := map[string]string{}
	for _, kv := range records[0].Attributes {
		attrs[kv.Key] = kv.Value.GetStringValue()
	}
	require.Equal(t, "caller\uFFFD", attrs["project"])
	require.Equal(t, "both", attrs["recovery"])
	var final CustomerTerminal
	require.NoError(t, json.Unmarshal([]byte(attrs["customer_request"]), &final))
	require.Equal(t, data.Customer.Snapshot(), final)
	require.Equal(t, "both", final.Recovery)
	require.Equal(t, "success", final.Outcome)
	require.Equal(t, "node-b", final.Provider)
}
