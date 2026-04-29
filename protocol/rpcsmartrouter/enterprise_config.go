//go:build enterprise

package rpcsmartrouter

import (
	"fmt"

	"github.com/Magma-Devs/smart-router/licensing"
	"github.com/Magma-Devs/smart-router/protocol/chainlib"
	spectypes "github.com/Magma-Devs/smart-router/types/spec"
)

// enterpriseConfig is the all-permissive capability set used when the binary
// is built with the enterprise tag and a valid (or grace-period) license is
// loaded at startup.
//
// Every gate returns nil/true; subscription factories return the real Direct*
// implementations as pure constructors — caller is responsible for Start()ing
// the manager (the wiring lives in rpcsmartrouter.go and is owned by Sprint 3).
type enterpriseConfig struct {
	license *licensing.License
}

func (enterpriseConfig) Edition() string                 { return "enterprise" }
func (e enterpriseConfig) License() *licensing.License   { return e.license }
func (enterpriseConfig) SupportsWSSubscriptions() bool   { return true }
func (enterpriseConfig) SupportsGRPCSubscriptions() bool { return true }

// All API interfaces are unlocked by any valid enterprise license.
func (enterpriseConfig) ValidateAPIInterface(apiInterface string) error {
	switch apiInterface {
	case spectypes.APIInterfaceJsonRPC,
		spectypes.APIInterfaceRest,
		spectypes.APIInterfaceGrpc,
		spectypes.APIInterfaceTendermintRPC:
		return nil
	default:
		// Even enterprise refuses values that aren't a known interface — this
		// catches typos in YAML rather than silently allowing an unknown handler.
		return fmt.Errorf("unsupported api-interface %q", apiInterface)
	}
}

// All transports are unlocked by any valid enterprise license.
func (enterpriseConfig) ValidateTransport(rawURL string) error { return nil }

// All specs are unlocked by any valid enterprise license.
func (enterpriseConfig) ValidateSpec(specIndex string) error { return nil }

func (enterpriseConfig) CreateWSSubscriptionManager(opts WSSubscriptionManagerOptions) (chainlib.WSSubscriptionManager, error) {
	return NewDirectWSSubscriptionManager(
		opts.Metrics,
		opts.ConnectionType,
		opts.ChainID,
		opts.APIInterface,
		opts.Endpoints,
		opts.Optimizer,
		opts.Config,
	), nil
}

func (enterpriseConfig) CreateGRPCSubscriptionManager(opts GRPCSubscriptionManagerOptions) (GRPCSubscriptionManager, error) {
	return NewDirectGRPCSubscriptionManager(
		opts.Metrics,
		opts.ChainID,
		opts.APIInterface,
		opts.Endpoints,
		opts.Optimizer,
		opts.Config,
	), nil
}
