package rpcsmartrouter

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Magma-Devs/smart-router/licensing"
	"github.com/Magma-Devs/smart-router/protocol/chainlib"
	"github.com/Magma-Devs/smart-router/protocol/common"
	"github.com/Magma-Devs/smart-router/protocol/metrics"
	spectypes "github.com/Magma-Devs/smart-router/types/spec"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/grpc"
)

// SmartRouterConfig is the capability gate for the smart router.
// All enterprise/community boundaries flow through this interface.
//
// Analytics is intentionally NOT in this interface — protocol/metrics
// is shared by both editions unchanged.
//
// The active implementation is selected at startup (see ActivateConfig);
// callers should reach it via ActiveConfig().
type SmartRouterConfig interface {
	Edition() string
	License() *licensing.License
	ValidateAPIInterface(apiInterface string) error
	ValidateTransport(rawURL string) error
	ValidateSpec(specIndex string) error
	SupportsWSSubscriptions() bool
	SupportsGRPCSubscriptions() bool
	CreateWSSubscriptionManager(opts WSSubscriptionManagerOptions) (chainlib.WSSubscriptionManager, error)
	CreateGRPCSubscriptionManager(opts GRPCSubscriptionManagerOptions) (GRPCSubscriptionManager, error)
}

// GRPCSubscriptionManager is the interface extracted over *DirectGRPCSubscriptionManager
// so a community noop can be substituted. The method set is dictated by real call sites
// in this package (rpcsmartrouter_server.go:196 GetGRPCReflectionConnection, and the
// streaming-method gate near rpcsmartrouter_server.go:1770).
//
// If a new call site appears, expand the interface — do not silently widen the noop.
type GRPCSubscriptionManager interface {
	GetReflectionConnection(ctx context.Context) (*grpc.ClientConn, func(), error)
	IsStreamingMethod(ctx context.Context, methodPath string) (bool, *desc.MethodDescriptor, error)
}

// WSSubscriptionManagerOptions bundles the inputs required to build a
// *DirectWSSubscriptionManager. Field names mirror the existing positional
// constructor at direct_ws_subscription_manager.go so the enterprise factory
// can unpack them without changing the constructor signature.
type WSSubscriptionManagerOptions struct {
	Metrics        metrics.ConsumerMetricsManagerInf
	ConnectionType string
	ChainID        string
	APIInterface   string
	Endpoints      []*common.NodeUrl
	Optimizer      WebSocketEndpointOptimizer
	Config         *WebsocketConfig
}

// GRPCSubscriptionManagerOptions bundles the inputs required to build a
// *DirectGRPCSubscriptionManager. Same shape rule as WSSubscriptionManagerOptions.
type GRPCSubscriptionManagerOptions struct {
	Metrics      metrics.ConsumerMetricsManagerInf
	ChainID      string
	APIInterface string
	Endpoints    []*common.NodeUrl
	Optimizer    WebSocketEndpointOptimizer
	Config       *GRPCStreamingConfig
}

// communityConfig is the default, restrictive capability set used when the
// binary is built without the enterprise tag, or when an enterprise build
// has no valid license. Community allows: jsonrpc only, http/https only,
// EVM-only specs (per community_specs.go).
type communityConfig struct{}

func (communityConfig) Edition() string                 { return "community" }
func (communityConfig) License() *licensing.License     { return nil }
func (communityConfig) SupportsWSSubscriptions() bool   { return false }
func (communityConfig) SupportsGRPCSubscriptions() bool { return false }

func (communityConfig) ValidateAPIInterface(apiInterface string) error {
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC:
		return nil
	case spectypes.APIInterfaceRest:
		return fmt.Errorf("REST interface requires an enterprise license — see https://github.com/Magma-Devs/smart-router#enterprise")
	case spectypes.APIInterfaceGrpc:
		return fmt.Errorf("gRPC interface requires an enterprise license — see https://github.com/Magma-Devs/smart-router#enterprise")
	case spectypes.APIInterfaceTendermintRPC:
		return fmt.Errorf("TendermintRPC interface requires an enterprise license — see https://github.com/Magma-Devs/smart-router#enterprise")
	default:
		return fmt.Errorf("unsupported api-interface %q", apiInterface)
	}
}

// ValidateTransport rejects WebSocket and explicitly-gRPC URL schemes.
// Bare-host gRPC URLs (where the upstream is gRPC but the URL has no scheme)
// are detected at the §3.3.6 row 3a insertion point in convertProvidersToSessions
// where the per-endpoint ApiInterface is in scope — this method only sees the
// raw URL string and cannot reliably distinguish a bare-host gRPC from a
// bare-host HTTP without that context.
func (communityConfig) ValidateTransport(rawURL string) error {
	scheme := schemeOf(rawURL)
	switch scheme {
	case "http", "https":
		return nil
	case "ws", "wss":
		return fmt.Errorf("WebSocket transport (url=%q) requires an enterprise license", rawURL)
	case "grpc", "grpcs":
		return fmt.Errorf("gRPC transport (url=%q) requires an enterprise license", rawURL)
	case "":
		return nil
	default:
		return fmt.Errorf("unsupported transport scheme %q (url=%q)", scheme, rawURL)
	}
}

func (communityConfig) ValidateSpec(specIndex string) error {
	if isCommunityAllowedSpec(specIndex) {
		return nil
	}
	return fmt.Errorf("non-EVM spec %q requires an enterprise license", specIndex)
}

func (communityConfig) CreateWSSubscriptionManager(opts WSSubscriptionManagerOptions) (chainlib.WSSubscriptionManager, error) {
	return NewNoOpWSSubscriptionManager(opts.ChainID, opts.APIInterface), nil
}

func (communityConfig) CreateGRPCSubscriptionManager(opts GRPCSubscriptionManagerOptions) (GRPCSubscriptionManager, error) {
	return newNoopGRPCSubscriptionManager(opts.ChainID, opts.APIInterface), nil
}

// schemeOf returns the lowercased URL scheme, or "" if the URL has no scheme.
// Uses url.Parse for correctness rather than HasPrefix games — bare-host inputs
// like "1.2.3.4:443" parse to opaque values and are returned as scheme "".
func schemeOf(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	if !strings.Contains(rawURL, "://") {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Scheme)
}

// enterpriseConfigFactory builds a SmartRouterConfig from a validated license.
// The function pointer is set by enterprise_features.go's init() (only present
// in enterprise builds), which keeps the community build free of any reference
// to enterpriseConfig.
type enterpriseConfigFactory func(*licensing.License) SmartRouterConfig

var (
	configMu          sync.RWMutex
	activeConfig      SmartRouterConfig = communityConfig{}
	enterpriseFactory enterpriseConfigFactory
)

// RegisterEnterpriseConfig wires the enterprise factory at process startup.
// Called from enterprise_features.go's init() in enterprise builds.
//
// Safe to call exactly once. Subsequent calls panic so we fail loudly if a
// future contributor accidentally registers two factories.
func RegisterEnterpriseConfig(f enterpriseConfigFactory) {
	configMu.Lock()
	defer configMu.Unlock()
	if enterpriseFactory != nil {
		panic("rpcsmartrouter: enterprise config factory already registered")
	}
	enterpriseFactory = f
}

// IsEnterpriseBuild reports whether this binary was built with the enterprise
// tag (and therefore registered an enterprise factory at init time).
func IsEnterpriseBuild() bool {
	configMu.RLock()
	defer configMu.RUnlock()
	return enterpriseFactory != nil
}

// ActivateConfig promotes activeConfig to enterpriseConfig if both an
// enterprise factory is registered and a non-nil license is provided.
// In every other case activeConfig stays as communityConfig.
//
// Idempotent — calling twice with the same license is a no-op.
func ActivateConfig(license *licensing.License) {
	configMu.Lock()
	defer configMu.Unlock()
	if license != nil && enterpriseFactory != nil {
		activeConfig = enterpriseFactory(license)
	}
}

// ActiveConfig returns the current capability gate. Always non-nil — defaults
// to communityConfig before ActivateConfig is called.
func ActiveConfig() SmartRouterConfig {
	configMu.RLock()
	defer configMu.RUnlock()
	return activeConfig
}
