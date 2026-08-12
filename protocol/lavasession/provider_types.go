package lavasession

import (
	"fmt"
	"sort"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/utils"
)

type NetworkAddressData struct {
	Address    string `yaml:"address,omitempty" json:"address,omitempty" mapstructure:"address,omitempty"` // HOST:PORT
	KeyPem     string `yaml:"key-pem,omitempty" json:"key-pem,omitempty" mapstructure:"key-pem"`
	CertPem    string `yaml:"cert-pem,omitempty" json:"cert-pem,omitempty" mapstructure:"cert-pem"`
	DisableTLS bool   `yaml:"disable-tls,omitempty" json:"disable-tls,omitempty" mapstructure:"disable-tls"`
}

type RPCProviderEndpoint struct {
	NetworkAddress NetworkAddressData `yaml:"network-address,omitempty" json:"network-address,omitempty" mapstructure:"network-address,omitempty"`
	ChainID        string             `yaml:"chain-id,omitempty" json:"chain-id,omitempty" mapstructure:"chain-id"` // spec chain identifier
	ApiInterface   string             `yaml:"api-interface,omitempty" json:"api-interface,omitempty" mapstructure:"api-interface"`
	NodeUrls       []common.NodeUrl   `yaml:"node-urls,omitempty" json:"node-urls,omitempty" mapstructure:"node-urls"`
	Name           string             `yaml:"provider-name,omitempty" json:"provider-name,omitempty" mapstructure:"provider-name"`
}

// RPCStaticProviderEndpoint extends RPCProviderEndpoint with additional fields for static providers
// This allows us to add functionality without modifying the original protobuf-derived type
type RPCStaticProviderEndpoint struct {
	NetworkAddress NetworkAddressData `yaml:"network-address,omitempty" json:"network-address,omitempty" mapstructure:"network-address,omitempty"`
	ChainID        string             `yaml:"chain-id,omitempty" json:"chain-id,omitempty" mapstructure:"chain-id"` // spec chain identifier
	ApiInterface   string             `yaml:"api-interface,omitempty" json:"api-interface,omitempty" mapstructure:"api-interface"`
	NodeUrls       []common.NodeUrl   `yaml:"node-urls,omitempty" json:"node-urls,omitempty" mapstructure:"node-urls"`
	Name           string             `yaml:"name,omitempty" json:"name,omitempty" mapstructure:"name,omitempty"`
	// Stake is an optional stake amount (in ulava) used for provider selection scoring in static-provider tests.
	// If omitted, it is treated as 0 so the weight calculator can apply the legacy
	// "static provider boost" behavior (instead of using an explicit stake value).
	Stake int64 `yaml:"stake,omitempty" json:"stake,omitempty" mapstructure:"stake,omitempty"`
	// GroupLabel is an optional, deployment-defined provider-group identifier (e.g. "tier-1", "external")
	// used by cross-validation group-diversity policies to require a quorum span N distinct groups.
	// Empty means the provider belongs to the implicit common.DefaultProviderGroup; it has no effect unless a
	// per-method policy sets a group-diversity requirement.
	GroupLabel string `yaml:"group-label,omitempty" json:"group-label,omitempty" mapstructure:"group-label,omitempty"`
}

// ToBase returns the base RPCProviderEndpoint (for compatibility with existing code)
func (ext *RPCStaticProviderEndpoint) ToBase() *RPCProviderEndpoint {
	return &RPCProviderEndpoint{
		NetworkAddress: ext.NetworkAddress,
		ChainID:        ext.ChainID,
		ApiInterface:   ext.ApiInterface,
		NodeUrls:       ext.NodeUrls,
	}
}

// Validate checks if the RPCStaticProviderEndpoint has valid configuration
func (ext *RPCStaticProviderEndpoint) Validate() error {
	if ext.Name == "" {
		return fmt.Errorf("provider name cannot be empty")
	}
	if len(ext.NodeUrls) == 0 {
		return fmt.Errorf("provider must have at least one node URL")
	}
	if ext.ChainID == "" {
		return fmt.Errorf("provider chain-id cannot be empty")
	}
	if ext.ApiInterface == "" {
		return fmt.Errorf("provider api-interface cannot be empty")
	}
	return nil
}

// providerNameScope is the set within which a provider name has to be unique. A session manager is
// built per chain+api-interface and owns its own pairing map, keyed by the provider's name, so that
// is exactly the scope where two entries sharing a name collapse into one slot. The same name under
// a different chain lands in a different session manager and is harmless — rejecting it would break
// the common convention of labelling every upstream with its operator ("alchemy" on both ETH1 and
// POLYGON1).
type providerNameScope struct {
	chainID      string
	apiInterface string
}

// ValidateUniqueProviderNames rejects a configuration in which two providers on the same
// chain+api-interface share a name. Pass every list that feeds one router — static and backup
// together — because they are looked up against each other by address and a name shared across the
// two is as ambiguous as one shared within either.
//
// A provider's name IS its routing identity: it is what the session manager keys csm.pairing by and
// what the retry skip-list holds. Two providers sharing one collapse into a single entry, so one
// upstream serves every request while the other sits idle, and setting that name aside after a
// failure removes both and leaves nothing to retry against (MAG-2724).
//
// The router therefore refuses to start rather than serving at half capacity on a config it cannot
// route unambiguously. The message names every collision so one edit fixes the config.
func ValidateUniqueProviderNames(lists ...[]*RPCStaticProviderEndpoint) error {
	grouped := map[providerNameScope]map[string][]*RPCStaticProviderEndpoint{}
	for _, list := range lists {
		for _, ext := range list {
			if ext == nil {
				continue
			}
			scope := providerNameScope{chainID: ext.ChainID, apiInterface: ext.ApiInterface}
			if grouped[scope] == nil {
				grouped[scope] = map[string][]*RPCStaticProviderEndpoint{}
			}
			grouped[scope][ext.Name] = append(grouped[scope][ext.Name], ext)
		}
	}

	// Every collision is reported, sorted — an operator who duplicated three names should have to
	// fix the config once, not boot three times to discover them one at a time. Sorted because the
	// grouping is a map: reporting in range order would make the same bad config produce a
	// different message on each run.
	var collisions []string
	for scope, byName := range grouped {
		for name, entries := range byName {
			if len(entries) < 2 {
				continue
			}
			urls := make([]string, 0, len(entries))
			for _, entry := range entries {
				if len(entry.NodeUrls) > 0 {
					urls = append(urls, entry.NodeUrls[0].Url)
				}
			}
			collisions = append(collisions, fmt.Sprintf(
				"%q on chain %s api-interface %s is shared by %d providers (urls: %s)",
				name, scope.chainID, scope.apiInterface, len(entries), strings.Join(urls, ", "),
			))
		}
	}
	if len(collisions) == 0 {
		return nil
	}
	sort.Strings(collisions)
	return fmt.Errorf(
		"duplicate provider names: %s — a provider name is the router's identity for a node, so two nodes sharing one on the same chain and api-interface cannot be told apart; give each a distinct name",
		strings.Join(collisions, "; "),
	)
}

func (endpoint *RPCProviderEndpoint) UrlsString() string {
	st_urls := make([]string, len(endpoint.NodeUrls))
	for idx, url := range endpoint.NodeUrls {
		st_urls[idx] = url.UrlStr()
	}
	return strings.Join(st_urls, ", ")
}

func (endpoint *RPCProviderEndpoint) AddonsString() string {
	st_urls := make([]string, len(endpoint.NodeUrls))
	for idx, url := range endpoint.NodeUrls {
		st_urls[idx] = strings.Join(url.Addons, ",")
	}
	return strings.Join(st_urls, "; ")
}

func (endpoint *RPCProviderEndpoint) String() string {
	return endpoint.ChainID + ":" + endpoint.ApiInterface + " Network Address:" + endpoint.NetworkAddress.Address + " Node:" + endpoint.UrlsString() + " Addons:" + endpoint.AddonsString()
}

func (endpoint *RPCProviderEndpoint) Validate() error {
	if len(endpoint.NodeUrls) == 0 {
		return utils.FormatError("Empty URL list for endpoint", nil, utils.Attribute{Key: "endpoint", Value: endpoint.String()})
	}
	for _, url := range endpoint.NodeUrls {
		err := common.ValidateEndpoint(url.Url, endpoint.ApiInterface)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rpcpe *RPCProviderEndpoint) Key() string {
	return rpcpe.ChainID + rpcpe.ApiInterface
}
