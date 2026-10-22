package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"unicode/utf8"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

type customerLogExporter struct{ records []sdklog.Record }

func (e *customerLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	for _, r := range records {
		e.records = append(e.records, r.Clone())
	}
	return nil
}
func (*customerLogExporter) Shutdown(context.Context) error   { return nil }
func (*customerLogExporter) ForceFlush(context.Context) error { return nil }

// The terminal snapshot reaches the collector on the usage event alone: relay_end is the one
// per-relay log line, and the Loki readers that count relays would count a second one twice.
func TestCustomerTerminalReachesOTelWithoutASecondLogLine(t *testing.T) {
	exporter := &customerLogExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sink := &OTelUsageSink{provider: provider, logger: provider.Logger("test")}
	recorder := &customerRecorder{}
	logger, err := NewRPCConsumerLogs(recorder, sink, nil)
	require.NoError(t, err)
	var stdout bytes.Buffer
	previous := zerologlog.Logger
	zerologlog.Logger = zerolog.New(&stdout).Level(zerolog.InfoLevel)
	t.Cleanup(func() { zerologlog.Logger = previous })
	data := NewRelayAnalytics("tenant", "ETH1", "jsonrpc")
	ctx := logger.BeginCustomerRequest(context.Background(), data)
	ObserveCustomerProvider(ctx, "https://example.com/v1?api_key=private-secret")
	logger.AddMetricForWebSocket(data, nil)
	logger.AddMetricForWebSocket(data, nil)
	require.Len(t, exporter.records, 1)
	require.Len(t, recorder.outcomes, 1)
	attributes := map[string]attribute.Value{}
	exporter.records[0].WalkAttributes(func(kv attribute.KeyValue) bool { attributes[string(kv.Key)] = kv.Value; return true })
	require.Equal(t, "customer", attributes["request_kind"].AsString())
	require.True(t, attributes["terminal"].AsBool())
	require.Equal(t, "success", attributes["outcome"].AsString())
	var exported CustomerTerminal
	require.NoError(t, json.Unmarshal([]byte(attributes["customer_request"].AsString()), &exported))
	require.Equal(t, recorder.outcomes[0], exported)
	require.Empty(t, stdout.String(), "a customer request ends in relay_end, not in a line of its own")
	require.NotContains(t, attributes["customer_request"].AsString(), "private-secret")
}

// Every string attribute of an exported usage record is valid UTF-8, whatever the request
// carried: OTLP marshals them as protobuf strings, and one invalid value fails the batch.
func TestUsageRecordAttributesAreValidUTF8(t *testing.T) {
	exporter := &customerLogExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	sink := &OTelUsageSink{provider: provider, logger: provider.Logger("test")}
	logger, err := NewRPCConsumerLogs(&customerRecorder{}, sink, nil)
	require.NoError(t, err)

	data := NewRelayAnalytics("acme\xff", "ETH1", "jsonrpc")
	data.ApiMethod = "Default-/x/\xfe"
	ctx := utils.WithRequestId(context.Background(), "req-\xff")
	logger.BeginCustomerRequest(ctx, data)
	logger.AddMetricForHttp(data, nil, map[string][]string{OriginHeaderKey: {"https://o\xff.example"}})
	require.Len(t, exporter.records, 1)

	strs := map[string]string{}
	exporter.records[0].WalkAttributes(func(kv attribute.KeyValue) bool {
		if v := kv.Value.AsString(); v != "" {
			strs[string(kv.Key)] = v
		}
		return true
	})
	for key, value := range strs {
		require.True(t, utf8.ValidString(value), "attribute %s = %q", key, value)
	}
	require.Equal(t, "acme\uFFFD", strs["project"], "the value is kept, not collapsed to other")
}
