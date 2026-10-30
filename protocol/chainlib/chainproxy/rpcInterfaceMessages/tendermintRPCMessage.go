package rpcInterfaceMessages

import (
	"fmt"
	"reflect"

	"github.com/goccy/go-json"

	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcclient"
	"github.com/magma-Devs/smart-router/utils"
)

type TendermintrpcMessage struct {
	JsonrpcMessage
	Path string
}

// get msg hash byte array containing all the relevant information for a unique request. (headers / api / params)
func (tm *TendermintrpcMessage) GetRawRequestHash() ([]byte, error) {
	return tm.rawRequestHashWithParams(tm.Method + tm.Path)
}

type TendermintMessageResponseBody struct {
	Code int    `json:"code,omitempty"`
	Log  string `json:"log,omitempty"`
}

type TendermintMessageResponse struct {
	Response TendermintMessageResponseBody `json:"response,omitempty"`
}

// CheckResponseError classifies a Tendermint JSON-RPC response. The envelope
// shape check (malformed bytes, missing-both-fields, error:null normalisation,
// top-level error extraction) is delegated to checkJsonrpcEnvelope; this
// method adds the Tendermint-specific success-path inspection of the embedded
// `{"response": {"code": N, "log": M}}` payload that Tendermint nodes use to
// signal application-level errors over an otherwise-200 envelope.
func (jm TendermintrpcMessage) CheckResponseError(data []byte, httpStatusCode int) (hasError bool, errorMessage string) {
	hasErr, msg, resultBytes := checkJsonrpcEnvelope(data, "Tendermint RPC")
	if hasErr || resultBytes == nil {
		return hasErr, msg
	}
	var inner TendermintMessageResponse
	if err := json.Unmarshal(resultBytes, &inner); err == nil {
		if inner.Response.Code != 0 && inner.Response.Log != "" {
			return true, inner.Response.Log
		}
	}
	return false, ""
}

// RPCError is the error object of a Tendermint JSON-RPC response, in the shape CometBFT's
// rpc/jsonrpc/types.RPCError has (the only thing the router used that module for).
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

func (err RPCError) Error() string {
	const baseFormat = "RPC error %v - %s"
	if err.Data != "" {
		return fmt.Sprintf(baseFormat+": %s", err.Code, err.Message, err.Data)
	}
	return fmt.Sprintf(baseFormat, err.Code, err.Message)
}

func GetTendermintRPCError(jsonError *rpcclient.JsonError) (*RPCError, error) {
	// Guard that the jsonError exists
	//nolint
	if jsonError == nil {
		return nil, nil
	}

	var rpcError *RPCError

	errData := ""
	var ok bool

	// Make sure jsonError.Data exists
	if jsonError.Data != nil {
		errData, ok = jsonError.Data.(string)
		if !ok {
			return nil, utils.LavaFormatError("(rpcMsg.Error.Data).(string) conversion failed", nil, utils.Attribute{Key: "data", Value: jsonError.Data})
		}
	}

	rpcError = &RPCError{
		Code:    jsonError.Code,
		Message: jsonError.Message,
		Data:    errData,
	}
	return rpcError, nil
}

func ConvertErrorToRPCError(errString string, code int) *RPCError {
	var rpcError *RPCError
	unmarshalError := json.Unmarshal([]byte(errString), &rpcError)
	if unmarshalError != nil || (rpcError.Data == "" && rpcError.Message == "") {
		utils.LavaFormatWarning("Failed unmarshalling error tendermintrpc", unmarshalError, utils.Attribute{Key: "err", Value: errString})
		rpcError = &RPCError{
			Code:    code,
			Message: "Rpc Error",
			Data:    errString,
		}
	}
	return rpcError
}

type jsonrpcId interface {
	isJSONRPCID()
}

// JSONRPCStringID a wrapper for JSON-RPC string IDs
type JSONRPCStringID string

func (JSONRPCStringID) isJSONRPCID()      {}
func (id JSONRPCStringID) String() string { return string(id) }

// JSONRPCIntID a wrapper for JSON-RPC integer IDs
type JSONRPCIntID int

func (JSONRPCIntID) isJSONRPCID()      {}
func (id JSONRPCIntID) String() string { return fmt.Sprintf("%d", id) }

func IdFromRawMessage(rawID json.RawMessage) (jsonrpcId, error) {
	var idInterface interface{}
	err := json.Unmarshal(rawID, &idInterface)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal id from response: %w (id: %s)", err, string(rawID))
	}

	switch id := idInterface.(type) {
	case string:
		return JSONRPCStringID(id), nil
	case float64:
		// json.Unmarshal uses float64 for all numbers
		return JSONRPCIntID(int(id)), nil
	case nil:
		return jsonrpcId(nil), nil
	default:
		typ := reflect.TypeOf(id)
		return nil, fmt.Errorf("failed to unmarshal id not a string or float (id: %s, type: %v)", string(rawID), typ)
	}
}

type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      jsonrpcId       `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

func ConvertTendermintMsg(rpcMsg *rpcclient.JsonrpcMessage) (*RPCResponse, error) {
	// Return an error if the message was not sent
	if rpcMsg == nil {
		return nil, ErrFailedToConvertMessage
	}
	rpcError, err := GetTendermintRPCError(rpcMsg.Error)
	if err != nil {
		return nil, err
	}

	jsonid, err := IdFromRawMessage(rpcMsg.ID)
	if err != nil {
		return nil, err
	}
	msg := &RPCResponse{
		JSONRPC: rpcMsg.Version,
		ID:      jsonid,
		Result:  rpcMsg.Result,
		Error:   rpcError,
	}

	return msg, nil
}

func ConvertToTendermintError(errString string, inputInfo []byte) string {
	var msg JsonrpcMessage
	err := json.Unmarshal(inputInfo, &msg)
	if err == nil {
		id, errId := IdFromRawMessage(msg.ID)
		if errId != nil {
			utils.LavaFormatError("error idFromRawMessage", errId)
			return chainproxy.InternalErrorString
		}
		res, merr := json.Marshal(&RPCResponse{
			JSONRPC: msg.Version,
			ID:      id,
			Error:   ConvertErrorToRPCError(errString, chainproxy.LavaErrorCode),
		})
		if merr != nil {
			utils.LavaFormatError("convertToTendermintError json.Marshal", merr)
			return chainproxy.InternalErrorString
		}
		return string(res)
	}
	utils.LavaFormatError("error convertToTendermintError", err)
	return chainproxy.InternalErrorString
}
