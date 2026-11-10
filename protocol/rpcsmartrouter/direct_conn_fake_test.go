package rpcsmartrouter

import (
	"context"
	"time"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
)

// fakeDirectConn is a scriptable DirectRPCConnection: it returns the response and error it was
// built with, and records the payload and the remaining context budget of the last request.
type fakeDirectConn struct {
	resp       *lavasession.DirectRPCResponse
	err        error
	gotPayload []byte
	gotTimeout time.Duration // remaining context budget observed by the request
}

func (f *fakeDirectConn) SendRequest(ctx context.Context, data []byte, headers map[string]string) (*lavasession.DirectRPCResponse, error) {
	f.gotPayload = data
	if deadline, ok := ctx.Deadline(); ok {
		f.gotTimeout = time.Until(deadline)
	}
	return f.resp, f.err
}
func (f *fakeDirectConn) GetProtocol() lavasession.DirectRPCProtocol { return "http" }
func (f *fakeDirectConn) Close() error                               { return nil }
func (f *fakeDirectConn) GetURL() string                             { return "http://fake:8545" }
func (f *fakeDirectConn) GetNodeUrl() *common.NodeUrl                { return nil }
