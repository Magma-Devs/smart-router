package rpcsmartrouter

import (
	"strings"

	"github.com/Magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/Magma-Devs/smart-router/types/spec"
	"github.com/Magma-Devs/smart-router/utils"
)

// validateSmartRouterConfigAgainstEdition is §3.3.6 row #8 — the centralized
// edition gate run once at startup. Community (default ActiveConfig) restricts
// API interfaces, specs, and transports; enterprise lets everything through.
//
// Defense in depth: the same checks run inline at provider-construction time
// (convertProvidersToSessions). This central pass guarantees the YAML is
// validated before any connections are built, even if a future caller bypasses
// the inline gates.
func validateSmartRouterConfigAgainstEdition(
	rpcEndpoints []*lavasession.RPCEndpoint,
	directRPCEndpoints []*lavasession.RPCStaticProviderEndpoint,
	backupDirectRPCEndpoints []*lavasession.RPCStaticProviderEndpoint,
) error {
	cfg := ActiveConfig()

	// API interface + spec — once per listener endpoint. Spec is deduped so a
	// chain configured for multiple interfaces only logs one rejection.
	seenSpecs := map[string]struct{}{}
	for _, ep := range rpcEndpoints {
		if err := cfg.ValidateAPIInterface(ep.ApiInterface); err != nil {
			return utils.LavaFormatError("api-interface rejected by edition", err,
				utils.Attribute{Key: "chainID", Value: ep.ChainID},
				utils.Attribute{Key: "apiInterface", Value: ep.ApiInterface})
		}
		if _, dup := seenSpecs[ep.ChainID]; !dup {
			seenSpecs[ep.ChainID] = struct{}{}
			if err := cfg.ValidateSpec(ep.ChainID); err != nil {
				return utils.LavaFormatError("spec rejected by edition", err,
					utils.Attribute{Key: "chainID", Value: ep.ChainID})
			}
		}
	}

	// Transport — every URL in every provider list. For bare-host URLs (no
	// '://' separator) AND ApiInterface == grpc, synthesize "grpc://" so
	// community rejects with the gRPC-transport error message.
	validateProviders := func(providers []*lavasession.RPCStaticProviderEndpoint) error {
		for _, provider := range providers {
			for _, url := range provider.NodeUrls {
				validateURL := url.Url
				if !strings.Contains(validateURL, "://") && provider.ApiInterface == spectypes.APIInterfaceGrpc {
					validateURL = "grpc://" + validateURL
				}
				if err := cfg.ValidateTransport(validateURL); err != nil {
					return utils.LavaFormatError("provider transport rejected by edition", err,
						utils.Attribute{Key: "url", Value: url.Url},
						utils.Attribute{Key: "provider", Value: provider.Name},
						utils.Attribute{Key: "apiInterface", Value: provider.ApiInterface})
				}
			}
		}
		return nil
	}
	if err := validateProviders(directRPCEndpoints); err != nil {
		return err
	}
	if err := validateProviders(backupDirectRPCEndpoints); err != nil {
		return err
	}
	return nil
}
