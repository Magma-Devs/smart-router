package qos

import "github.com/magma-Devs/smart-router/utils"

// The tests log at the router's default --log-level, info. utils starts at debug, where every relay
// the tests feed in logs a QoS report.
func init() {
	utils.SetGlobalLoggingLevel("info")
}
