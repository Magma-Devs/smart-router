package chainlib

import "github.com/magma-Devs/smart-router/utils"

// StatefulLogAttr is the `stateful` field "Choosing providers" writes - the api's
// stateful category, 1 for a write sent to every provider - for the other lines of
// the same relay. Log collectors label a write's lines by it, so a reader finds a
// transaction's lines without reading every relay's.
func StatefulLogAttr(protocolMessage ProtocolMessage) utils.Attribute {
	var stateful uint32
	if protocolMessage != nil {
		if api := protocolMessage.GetApi(); api != nil {
			stateful = api.Category.Stateful
		}
	}
	return utils.LogAttr("stateful", stateful)
}
