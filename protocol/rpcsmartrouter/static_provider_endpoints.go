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
		utils.LavaFormatFatal("could not unmarshal extended endpoints", err, utils.Attribute{Key: "viper_endpoints", Value: viperEndpoints.AllSettings()})
	}
	for _, endpoint := range endpoints {
		// Validate that the provider name is not empty
		if err := endpoint.Validate(); err != nil {
			return nil, utils.LavaFormatError("invalid provider configuration", err,
				utils.Attribute{Key: "chainID", Value: endpoint.ChainID},
				utils.Attribute{Key: "apiInterface", Value: endpoint.ApiInterface})
		}
	}
	return endpoints, err
}
