package common

import "github.com/magma-Devs/smart-router/utils"

// LogRelayStep writes a line every relay passes through on its way to the nodes,
// with the `stateful` field log collectors label a write's lines by. A write
// keeps the line at Info, so a transaction is followed at the default level. A
// read writes it at Debug: at Info, one such line per relay is a tenth of a
// router's log bytes.
func LogRelayStep(stateful uint32, description string, attributes ...utils.Attribute) {
	severity := uint(utils.LAVA_LOG_DEBUG)
	if stateful != NO_STATE {
		severity = utils.LAVA_LOG_INFO
	}
	utils.LavaFormatLog(description, nil, append(attributes, utils.LogAttr("stateful", stateful)), severity)
}
