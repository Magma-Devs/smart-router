package rpcsmartrouter

import (
	"fmt"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/performance"
	"github.com/magma-Devs/smart-router/utils"
)

// relayCacheMode is what --disable-relay-cache means beside the rest of the cache configuration.
//
// With the flag set, the relay path asks no cache tier and writes to none. The primary backend,
// if one is configured, is then used only by what --shared-state shares over a channel of its
// own: the sticky-session claims and the tracker's poll observations. The seen-block exchange
// rides on the relay cache calls themselves, so it is not made in this mode; a block a peer
// polled still reaches this replica's chain tip with the poll observation that carries it.
type relayCacheMode struct {
	disabled            bool
	primaryConfigured   bool
	secondaryConfigured bool
	sharedState         bool
}

func relayCacheModeFrom(disabled, primaryConfigured, secondaryConfigured, sharedState bool) relayCacheMode {
	return relayCacheMode{disabled: disabled, primaryConfigured: primaryConfigured, secondaryConfigured: secondaryConfigured, sharedState: sharedState}
}

// validate refuses the flag beside a secondary cache. That tier only serves cached replies, so
// the two settings contradict each other and neither is the obvious one to drop.
func (m relayCacheMode) validate() error {
	if m.disabled && m.secondaryConfigured {
		return fmt.Errorf("--%s is set together with a secondary cache (%s or the %s block); a secondary cache only serves cached replies, so keep one of the two",
			common.DisableRelayCacheFlag, performance.SecondaryCacheFlagName, performance.SecondaryRespCacheViperKey)
	}
	return nil
}

// advisory is the one startup line for a router with the flag set, and whether it is a warning:
// a backend that nothing uses is a configuration to look at, not a failure.
func (m relayCacheMode) advisory() (message string, warning bool) {
	switch {
	case !m.disabled:
		return "", false
	case !m.primaryConfigured:
		return "relay caching is off (--" + common.DisableRelayCacheFlag + ") and no cache backend is configured: nothing to turn off", false
	case !m.sharedState:
		return "relay caching is off (--" + common.DisableRelayCacheFlag + ") without --" + common.SharedStateFlag + ": the cache backend is connected and nothing uses it", true
	default:
		return "relay caching is off (--" + common.DisableRelayCacheFlag + "): the cache backend carries shared state only (sticky-session claims and tracker poll observations)", false
	}
}

func (m relayCacheMode) logStartup() {
	message, warning := m.advisory()
	switch {
	case message == "":
	case warning:
		utils.LavaFormatWarning(message, nil)
	default:
		utils.LavaFormatInfo(message)
	}
}
