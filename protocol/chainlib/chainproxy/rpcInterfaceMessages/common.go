package rpcInterfaceMessages

import (
	"errors"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"github.com/magma-Devs/smart-router/utils/sigs"
)

// rawRequestHash hashes what makes a request unique: its identity (method or path), its body and
// the forwarded headers, in that order. kind names the message type on the error line.
func rawRequestHash(kind string, headers []pairingtypes.Metadata, identity string, body []byte) ([]byte, error) {
	headersByteArray, err := json.Marshal(headers)
	if err != nil {
		utils.LavaFormatError("Failed marshalling headers on "+kind+" message", err, utils.LogAttr("headers", common.RedactMetadata(headers)))
		return []byte{}, err
	}
	return sigs.HashMsg(append(append([]byte(identity), body...), headersByteArray...)), nil
}

var WontCalculateBatchHash = errors.New("wont calculate batch message hash") // a batch has no single raw hash; whether it is retried is the policy's IsBatch rule, not this error (MAG-4242).

type ParsableRPCInput struct {
	Result json.RawMessage
	Error  *rpcclient.JsonError
	chainproxy.BaseMessage
}

func (pri ParsableRPCInput) GetParams() interface{} {
	return nil
}

func (pri ParsableRPCInput) GetMethod() string {
	return ""
}

func (pri ParsableRPCInput) GetResult() json.RawMessage {
	return pri.Result
}

func (pri ParsableRPCInput) GetID() json.RawMessage {
	return nil
}

func (pri ParsableRPCInput) GetError() *rpcclient.JsonError {
	return pri.Error
}

type GenericMessage interface {
	GetHeaders() []pairingtypes.Metadata
	DisableErrorHandling()
	GetParams() interface{}
}
