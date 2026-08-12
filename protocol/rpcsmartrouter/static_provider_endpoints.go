package rpcsmartrouter

import (
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/spf13/viper"
)

// ParseStaticProviderEndpoints parses static provider configuration into extended endpoint types.
func ParseStaticProviderEndpoints(viperEndpoints *viper.Viper, endpointsConfigName string) (endpoints []*lavasession.RPCStaticProviderEndpoint, err error) {
	err = viperEndpoints.UnmarshalKey(endpointsConfigName, &endpoints)
	if err != nil {
		utils.FormatFatal("could not unmarshal extended endpoints", err, utils.Attribute{Key: "viper_endpoints", Value: viperEndpoints.AllSettings()})
	}
	for _, endpoint := range endpoints {
		// Validate that the provider name is not empty
		if err := endpoint.Validate(); err != nil {
			return nil, utils.FormatError("invalid provider configuration", err,
				utils.Attribute{Key: "chainID", Value: endpoint.ChainID},
				utils.Attribute{Key: "apiInterface", Value: endpoint.ApiInterface})
		}
	}
	// Deliberately NOT checked here: duplicate provider names (MAG-2724). A name collision is a
	// property of the whole set of lists a router loads, not of one list — static and backup are
	// looked up against each other by address, so only a check over both together is complete, and
	// the boot path runs exactly that (see ValidateUniqueProviderNames in CreateSmartRouterCobraCommand).
	// Keeping this function purely a parser also keeps `smart-router health` usable: it is the tool
	// an operator reaches for to diagnose a config that will not boot, so it must be able to load
	// one.
	return endpoints, err
}
