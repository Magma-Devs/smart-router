package metrics

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// recordedSuccess keeps the Success each SetRelayMetrics call was handed.
type recordedSuccess struct {
	NoOpConsumerMetrics
	successes []bool
}

func (r *recordedSuccess) SetRelayMetrics(relayMetric *RelayMetrics, _ error) {
	r.successes = append(r.successes, relayMetric.Success)
}

// TestListenerHooksReadTheNodeErrorVerdict: every listener hands its request to one of three hooks
// with the error the relay returned. A relay that ended on a node's error answer returns no error —
// the client receives the node's reply — so each hook must also read EndedOnNodeError, or that
// request is counted as a customer success (MAG-4374). The three hooks are twins; each is pinned.
func TestListenerHooksReadTheNodeErrorVerdict(t *testing.T) {
	hooks := map[string]func(logs *RPCConsumerLogs, data *RelayMetrics, err error){
		"http":      func(logs *RPCConsumerLogs, data *RelayMetrics, err error) { logs.AddMetricForHttp(data, err, nil) },
		"websocket": func(logs *RPCConsumerLogs, data *RelayMetrics, err error) { logs.AddMetricForWebSocket(data, err) },
		"grpc": func(logs *RPCConsumerLogs, data *RelayMetrics, err error) {
			logs.AddMetricForGrpc(data, err, &metadata.MD{})
		},
	}
	for name, hook := range hooks {
		t.Run(name, func(t *testing.T) {
			recorder := &recordedSuccess{}
			logs, err := NewRPCConsumerLogs(recorder, nil, nil)
			require.NoError(t, err)

			hook(logs, &RelayMetrics{ApiMethod: "m"}, nil)
			hook(logs, &RelayMetrics{ApiMethod: "m", EndedOnNodeError: true}, nil)
			hook(logs, &RelayMetrics{ApiMethod: "m"}, errors.New("failed relay"))
			require.Equal(t, []bool{true, false, false}, recorder.successes)
		})
	}
}
