package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

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

func TestCustomerTerminalSameSnapshotInStdoutAndOTel(t *testing.T) {
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
	var line map[string]string
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &line))
	require.Equal(t, "relay finished", line["message"])
	require.Equal(t, "true", line["terminal"])
	var logged CustomerTerminal
	require.NoError(t, json.Unmarshal([]byte(line["customer_request"]), &logged))
	require.Equal(t, exported, logged)
	require.NotContains(t, stdout.String(), "private-secret")
	require.NotContains(t, attributes["customer_request"].AsString(), "private-secret")
}
