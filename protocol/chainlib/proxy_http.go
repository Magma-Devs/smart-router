package chainlib

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	common "github.com/magma-Devs/smart-router/protocol/common"
	pairingtypes "github.com/magma-Devs/smart-router/types/relay"
	"github.com/magma-Devs/smart-router/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// nodeHTTPClient is the client the REST, GraphQL and Tendermint URI proxies send with. It is
// built once for all of them: each proxy used to create its own on first send, unsynchronised,
// so two concurrent first sends raced on the field.
var nodeHTTPClient = sync.OnceValue(common.OptimizedHttpClient)

// nodeHTTPReply is the node's answer to doHTTP.
type nodeHTTPReply struct {
	status int
	header http.Header
	body   []byte
}

// doHTTP sends req to the proxy's node: it applies the message's headers (an empty value deletes
// the header), the url's auth headers and IP forwarding, then logRequest if set, and sends. A
// transport failure is classified by HandleNodeError; a status the error handler rejects comes
// back as an error carrying the node's Retry-After. The caller builds req, Content-Type
// included, so a message header can still override it.
func (bcp *BaseChainProxy) doHTTP(ctx context.Context, req *http.Request, nodeHeaders []pairingtypes.Metadata, disableErrorHandling bool, apiName string, logRequest func(http.Header)) (*nodeHTTPReply, error) {
	for _, header := range nodeHeaders {
		if header.Value == "" {
			req.Header.Del(header.Name)
		} else {
			req.Header.Set(header.Name, header.Value)
		}
	}
	bcp.NodeUrl.SetAuthHeaders(ctx, req.Header.Set)
	bcp.NodeUrl.SetIpForwardingIfNecessary(ctx, req.Header.Set)
	if logRequest != nil {
		logRequest(req.Header)
	}

	res, err := nodeHTTPClient().Do(req)
	if res != nil {
		// resp can be non nil on error
		grpc.SetTrailer(ctx, metadata.Pairs(common.StatusCodeMetadataKey, strconv.Itoa(res.StatusCode))) // we ignore this error here since this code can be triggered not from grpc
	}
	if err != nil {
		// Validate if the error is related to the provider connection to the node or it is a valid error
		// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
		if parsedError := bcp.HandleNodeError(ctx, err); parsedError != nil {
			return nil, parsedError
		}
		return nil, err
	}
	// here we received a response that can be an error response with Code >300 or code < 200
	if res.Body != nil {
		defer res.Body.Close()
	}

	if err := bcp.HandleStatusError(res.StatusCode, disableErrorHandling); err != nil {
		err = common.WithRetryAfter(err, res.Header, time.Now())
		return nil, utils.LavaFormatWarning("Received invalid status code", err,
			utils.Attribute{Key: "Status Code", Value: res.StatusCode},
			utils.Attribute{Key: "chainID", Value: bcp.ChainID},
			utils.Attribute{Key: "apiName", Value: apiName},
		)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return &nodeHTTPReply{status: res.StatusCode, header: res.Header, body: body}, nil
}

// applyRPCHeaders sets the message's headers on a pooled rpc client, deleting the ones the spec
// nullifies (an empty value), and returns what takes them off again, so the next borrower of the
// client does not inherit them. Setting an empty value would send the header empty instead of
// dropping it.
func applyRPCHeaders(rpc *rpcclient.Client, headers []pairingtypes.Metadata) (restore func()) {
	for _, header := range headers {
		if header.Value == "" {
			rpc.DelHeader(header.Name)
		} else {
			rpc.SetHeader(header.Name, header.Value)
		}
	}
	return func() {
		for _, header := range headers {
			rpc.DelHeader(header.Name)
		}
	}
}

// callNodeRPC sends one JSON-RPC call over a pooled rpc client and returns the node's message.
// isJsonRPC selects the JSON-RPC client rule that turns a non-2xx answer into an HTTPError, from
// which a node's JSON-RPC error body is recovered.
func (cp *JrpcChainProxy) callNodeRPC(ctx context.Context, chainMessage ChainMessageForSend, id []byte, method string, params interface{}, headers []pairingtypes.Metadata, isJsonRPC, disableErrorHandling bool, unrecoveredAttrs ...utils.Attribute) (*rpcclient.JsonrpcMessage, error) {
	rpc, err := cp.conn.GetRpc(ctx, true)
	if err != nil {
		return nil, err
	}
	// return the rpc connection to the pool after the function completes
	defer cp.conn.ReturnRpc(rpc)

	// appending hashed url
	grpc.SetTrailer(ctx, metadata.Pairs(RPCProviderNodeAddressHash, cp.conn.GetUrlHash()))

	defer applyRPCHeaders(rpc, headers)()
	// we use the minimum timeout between the two, spec or context. to prevent the node from hanging
	// we don't use the context alone so it won't be hanging forever by an attack
	connectCtx, cancel := cp.CapTimeoutForSend(ctx, chainMessage)
	defer cancel()

	cp.NodeUrl.SetIpForwardingIfNecessary(ctx, rpc.SetHeader)
	rpcMessage, nodeErr := rpc.CallContext(connectCtx, id, method, params, isJsonRPC, disableErrorHandling)
	if nodeErr == nil {
		return rpcMessage, nil
	}
	// here we are getting an error for every code that is not 200-300
	if errors.Is(nodeErr, common.StatusCodeError504) || errors.Is(nodeErr, common.StatusCodeError429) || errors.Is(nodeErr, common.StatusCodeErrorStrict) {
		return nil, utils.LavaFormatWarning("Received invalid status code", nodeErr, utils.Attribute{Key: "chainID", Value: cp.BaseChainProxy.ChainID}, utils.Attribute{Key: "apiName", Value: chainMessage.GetApi().Name})
	}
	// Validate if the error is related to the connection to the node or it is a valid error
	// in case the error is valid (e.g. bad input parameters) the error will return in the form of a valid error reply
	if parsedError := cp.HandleNodeError(ctx, nodeErr); parsedError != nil {
		return nil, parsedError
	}
	// try to parse node error as json message
	if rpcMessage = TryRecoverNodeErrorFromClientError(nodeErr); rpcMessage == nil {
		utils.LavaFormatDebug("got error from node", append([]utils.Attribute{utils.LogAttr("GUID", ctx), utils.LogAttr("nodeErr", nodeErr)}, unrecoveredAttrs...)...)
		return nil, nodeErr
	}
	return rpcMessage, nil
}
