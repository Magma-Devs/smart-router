package chainlib

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// handleConnectionError runs only on handleAndClassify's Unknown branch, so a connection shape
// common.DetectConnectionError names never reaches it. These are the shapes it used to reword as
// well; each one is classified centrally and returned as its LavaError, not as reworded text.
func TestConnectionShapes_AreDecidedByDetectConnectionError(t *testing.T) {
	refused := errors.New("connect: connection refused")
	shapes := map[string]error{
		"io.EOF":                    io.EOF,
		"http served as https":      errors.New(`Get "https://node": http: server gave HTTP response to HTTPS client`),
		"net.OpError timeout":       &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net.OpError refused":       &net.OpError{Op: "dial", Net: "tcp", Err: refused},
		"url.Error timeout":         &url.Error{Op: "Post", URL: "https://node", Err: os.ErrDeadlineExceeded},
		"url.Error refused":         &url.Error{Op: "Post", URL: "https://node", Err: refused},
		"net.DNSError no such host": &net.DNSError{Err: "no such host", Name: "node"},
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			connError := common.DetectConnectionError(shape)
			require.NotNil(t, connError, "the shared detector names this shape")

			result := handleAndClassify(context.Background(), shape, common.TransportJsonRPC, common.ChainFamilyEVM, "ETH1", &genericErrorHandler{})
			assert.ErrorIs(t, result, connError, "classified centrally, never reworded by handleConnectionError")
		})
	}
}

// What handleConnectionError still decides: shapes the shared detector does not name.
func TestHandleConnectionError_RewordsOnlyTheShapesNothingElseNames(t *testing.T) {
	geh := &genericErrorHandler{}
	for _, shape := range []error{
		net.ErrClosed,
		net.ErrWriteToConnected,
		&net.OpError{Op: "write", Net: "tcp", Err: errors.New("broken pipe")},
	} {
		require.Nil(t, common.DetectConnectionError(shape), "%v", shape)
		assert.NotNil(t, geh.handleConnectionError(shape), "%v", shape)
	}
}
