//go:build enterprise

package rpcsmartrouter

import (
	"github.com/Magma-Devs/smart-router/licensing"
)

// init registers the enterprise config factory at process startup. The community
// build excludes this file (no //go:build enterprise tag), leaving
// enterpriseFactory nil and activeConfig as communityConfig.
//
// Registration only — no method bodies. The factory itself constructs an
// enterpriseConfig (defined in enterprise_config.go).
func init() {
	RegisterEnterpriseConfig(func(l *licensing.License) SmartRouterConfig {
		return enterpriseConfig{license: l}
	})
}
