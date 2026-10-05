package rpcInterfaceMessages

import (
	"errors"

	"github.com/magma-Devs/smart-router/protocol/common"
)

// ErrJsonrpcBatchRefused is returned for a JSON-RPC batch the chain cannot take. Its text is what the
// client is told.
var ErrJsonrpcBatchRefused = errors.New("this chain does not accept that request inside a JSON-RPC batch; send it as a single request")

// jsonrpcChainFamilyRules is what a chain family changes about reading a JSON-RPC exchange, keyed by
// the families the error registry uses, so the JSON-RPC parser stays chain-agnostic. A family
// without an entry gets the envelope rule and accepts any batch.
var jsonrpcChainFamilyRules = map[common.ChainFamily]struct {
	responseErrorChecker func(msg *JsonrpcMessage) func(data []byte, httpStatusCode int) (hasError bool, errorMessage string)
	checkBatch           func(msgs []JsonrpcMessage) error
}{
	common.ChainFamilyXRP: {
		responseErrorChecker: func(msg *JsonrpcMessage) func(data []byte, httpStatusCode int) (bool, string) {
			return msg.CheckXRPLResponseError
		},
		checkBatch: checkXRPLBatch,
	},
}

// JsonrpcResponseErrorChecker returns the classifier for a single JSON-RPC reply on a chain of the
// given family.
func JsonrpcResponseErrorChecker(family common.ChainFamily, msg *JsonrpcMessage) func(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	if rules, ok := jsonrpcChainFamilyRules[family]; ok && rules.responseErrorChecker != nil {
		return rules.responseErrorChecker(msg)
	}
	return msg.CheckResponseError
}

// CheckJsonrpcBatch returns an error wrapping ErrJsonrpcBatchRefused when a chain of the given family
// cannot take this batch.
func CheckJsonrpcBatch(family common.ChainFamily, msgs []JsonrpcMessage) error {
	if rules, ok := jsonrpcChainFamilyRules[family]; ok && rules.checkBatch != nil {
		return rules.checkBatch(msgs)
	}
	return nil
}
