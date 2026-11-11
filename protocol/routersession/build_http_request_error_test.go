package routersession

import (
	"context"
	"net/http"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// MAG-4718: the router's protocol-error count leaves out a request the router could not even
// build, and tells one apart only by ErrBuildHTTPRequest, because the senders wrap the error like
// any error from the wire. TestIsProtocolFailure builds the sentinel itself, so this is the test
// that pins it where it is produced: without the mark, the router's own refusals would count as
// the upstream failing on the wire, and every other test would still pass.
func TestDoHTTPRequest_ARequestThatCannotBeBuiltCarriesErrBuildHTTPRequest(t *testing.T) {
	h := newHTTPConnection(t, common.NodeUrl{Url: "http://127.0.0.1:1"})
	for name, params := range map[string]HTTPRequestParams{
		"invalid method":               {Method: "BAD METHOD", URL: "http://127.0.0.1:1/"},
		"control character in the url": {Method: http.MethodGet, URL: "http://127.0.0.1:1/\x7f"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := h.DoHTTPRequest(context.Background(), params)
			require.ErrorIs(t, err, ErrBuildHTTPRequest)
			require.ErrorContains(t, err, "failed to build HTTP request: ", "the message is unchanged")
		})
	}
}
