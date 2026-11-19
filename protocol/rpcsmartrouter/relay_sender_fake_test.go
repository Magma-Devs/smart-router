package rpcsmartrouter

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/chainlib"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/stretchr/testify/require"
)

// routerRelaySender stands in for the router behind a real listener. ParseRelay is the router's
// own, rpcss.ParseRelay, unless parse replaces it. SendRelay refuses, unless keep is set: then
// it parses exactly what the listener handed over the same way, keeps the protocol message and
// answers with an empty body instead of relaying upstream. manager, when set, serves the
// listener's gRPC streams. SendParsedRelay is not used.
type routerRelaySender struct {
	rpcss   *RPCSmartRouterServer
	parse   func(req string) (chainlib.ProtocolMessage, error)
	keep    bool
	manager *DirectGRPCSubscriptionManager

	mu     sync.Mutex
	parsed []chainlib.ProtocolMessage
}

var errRelayPathNotUsed = errors.New("not used")

func (s *routerRelaySender) SendRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, analytics *metrics.RelayMetrics, metadataValues []pairingtypes.Metadata) (*common.RelayResult, error) {
	if !s.keep {
		return nil, errRelayPathNotUsed
	}
	protocolMessage, err := s.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadataValues)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.parsed = append(s.parsed, protocolMessage)
	s.mu.Unlock()
	return &common.RelayResult{Reply: &pairingtypes.RelayReply{Data: []byte{}}}, nil
}

func (s *routerRelaySender) ParseRelay(ctx context.Context, url, req, connectionType, dappID, consumerIp string, metadataValues []pairingtypes.Metadata) (chainlib.ProtocolMessage, error) {
	if s.parse != nil {
		return s.parse(req)
	}
	return s.rpcss.ParseRelay(ctx, url, req, connectionType, dappID, consumerIp, metadataValues)
}

func (s *routerRelaySender) SendParsedRelay(context.Context, *metrics.RelayMetrics, chainlib.ProtocolMessage) (*common.RelayResult, error) {
	return nil, errRelayPathNotUsed
}

// GetGRPCSubscriptionManager makes the sender a chainlib.GRPCSubscriptionProvider; nil leaves
// the listener without streaming, as on a router that has none.
func (s *routerRelaySender) GetGRPCSubscriptionManager() chainlib.GRPCSubscriptionManager {
	if s.manager == nil {
		return nil
	}
	return s.manager
}

// only is the one protocol message SendRelay kept; the test fails unless exactly one call reached
// the router.
func (s *routerRelaySender) only(t *testing.T) chainlib.ProtocolMessage {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Len(t, s.parsed, 1, "the call must reach the router exactly once")
	return s.parsed[0]
}
