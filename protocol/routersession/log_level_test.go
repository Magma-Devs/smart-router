package routersession

import "github.com/magma-Devs/smart-router/utils"

// testLogLevel is the router's default --log-level. utils starts at debug, where the session manager
// logs every pairing update, block and probe. captureLogs raises it to debug while it captures.
const testLogLevel = "info"

func init() {
	utils.SetGlobalLoggingLevel(testLogLevel)
}
