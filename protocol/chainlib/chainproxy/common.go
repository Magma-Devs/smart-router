package chainproxy

import (
	"fmt"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/protocol/parser"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

const (
	LavaErrorCode       = 555
	InternalErrorString = "Internal Error"
)

type CustomParsingMessage interface {
	NewParsableRPCInput(input json.RawMessage) (parser.RPCInput, error)
}

type BaseMessage struct {
	Headers                 []pairingtypes.Metadata
	LatestBlockHeaderSetter *spectypes.ParseDirective
	disableErrorHandling    bool
}

func (bm *BaseMessage) DisableErrorHandling() {
	bm.disableErrorHandling = true
}

func (bm *BaseMessage) GetDisableErrorHandling() bool {
	return bm.disableErrorHandling
}

// appends only headers that do not exist
func (bm *BaseMessage) AppendHeader(metadata []pairingtypes.Metadata) {
	existing := map[string]struct{}{}
	for _, metadataEntry := range bm.Headers {
		existing[metadataEntry.Name] = struct{}{}
	}
	for _, metadataEntry := range metadata {
		if _, ok := existing[metadataEntry.Name]; !ok {
			bm.Headers = append(bm.Headers, metadataEntry)
		}
	}
}

func (bm *BaseMessage) SetLatestBlockWithHeader(latestBlock uint64, modifyContent bool) (done bool) {
	if bm.LatestBlockHeaderSetter == nil {
		return false
	}
	headerValue := fmt.Sprintf(bm.LatestBlockHeaderSetter.FunctionTemplate, latestBlock)
	for idx, header := range bm.Headers {
		if header.Name == bm.LatestBlockHeaderSetter.ApiName {
			if modifyContent {
				bm.Headers[idx].Value = headerValue
			}
			return true
		}
	}
	if modifyContent {
		bm.Headers = append(bm.Headers, pairingtypes.Metadata{
			Name:  bm.LatestBlockHeaderSetter.ApiName,
			Value: headerValue,
		})
	}
	return true
}

func (bm BaseMessage) GetHeaders() []pairingtypes.Metadata {
	return bm.Headers
}

// ParseBlock parses a default block parameter ("latest", "earliest", a number) to a block
// number. Every message type shares this parser.
func (bm BaseMessage) ParseBlock(inp string) (int64, error) {
	return parser.ParseDefaultBlockParameter(inp)
}

// UpdateLatestBlockInMessage never rewrites a request: no message type pins "latest" to a
// concrete block. Setting it by header (SetLatestBlockWithHeader) stays off until the Cosmos SDK
// header behaves consistently, and a GraphQL document is forwarded as written, so pinning a block
// would mean changing the caller's query text.
func (bm *BaseMessage) UpdateLatestBlockInMessage(latestBlock uint64, modifyContent bool) (success bool) {
	return false
}

type DefaultRPCInput struct {
	Result json.RawMessage
	BaseMessage
}

func (dri DefaultRPCInput) GetParams() interface{} {
	return nil
}

func (dri DefaultRPCInput) GetMethod() string {
	return ""
}

func (dri DefaultRPCInput) GetResult() json.RawMessage {
	return dri.Result
}

func (dri DefaultRPCInput) GetID() json.RawMessage {
	return nil
}

func (dri DefaultRPCInput) GetError() *rpcclient.JsonError {
	return nil
}

func DefaultParsableRPCInput(input json.RawMessage) parser.RPCInput {
	return DefaultRPCInput{Result: input}
}
