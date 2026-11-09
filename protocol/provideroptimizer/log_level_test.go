package provideroptimizer

import "github.com/magma-Devs/smart-router/utils"

// The tests log at the router's default --log-level, info. utils starts at debug, where the selector
// logs every pick: millions of lines per run that nobody reads.
func init() {
	utils.SetGlobalLoggingLevel("info")
}
