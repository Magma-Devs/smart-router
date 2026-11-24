package chainlib

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/chainlib/chainproxy/rpcInterfaceMessages"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestGraphQLListener_AMethodOtherThanPostIsRefused is MAG-4524's GraphQL part, the GraphQL chapter's
// method check (case T1): a GET, a PUT, a PATCH, a DELETE or a HEAD, at any path but the health
// path, gets 405 with Allow: POST and the router's own reply, one GraphQL error with the code
// REQUEST_REFUSED, its guid in extensions and in Router-Guid. No provider is asked. A HEAD gets the
// status and the headers, since its reply has no body.
//
// The router used to answer a DELETE with 204 and an empty body, before any route ran, and did
// nothing, so a client bug read as success. A GET, a PUT and a PATCH got 405 with a body that holds
// no errors list, no Allow and no Router-Guid. OPTIONS keeps its 204, the health path its health
// check, and a POST is served at any path.
func TestGraphQLListener_AMethodOtherThanPostIsRefused(t *testing.T) {
	relay := &parsingRelay{}
	send := sendToGraphQLListener(t, relay)
	const request = `{"query":"{ chainIdentifier }"}`

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead} {
		for _, path := range []string{"/", "/graphql", "/x/y"} {
			t.Run(method+" "+path, func(t *testing.T) {
				relayed := relay.relayed
				body := request
				if method == http.MethodGet || method == http.MethodHead {
					body = ""
				}
				reply := send(method, path, body)

				require.Equal(t, http.StatusMethodNotAllowed, reply.status, "body: %s", reply.body)
				require.Equal(t, http.MethodPost, reply.header.Get(fiber.HeaderAllow), "the reply names the one method that is served")
				guid := reply.header.Get(common.GUID_HEADER_NAME)
				require.NotEmpty(t, guid, "the reply carries its request id in Router-Guid")
				require.Equal(t, relayed, relay.relayed, "a refused method reaches no provider")
				if method == http.MethodHead {
					require.Empty(t, reply.body, "a HEAD reply has no body")
					return
				}
				require.Equal(t, fiber.MIMEApplicationJSONCharsetUTF8, reply.header.Get(fiber.HeaderContentType))
				entry := requireGraphQLErrorEntry(t, reply.body)
				require.Equal(t, rpcInterfaceMessages.GraphQLRequestRefusedCode, entry.Extensions["code"], "body: %s", reply.body)
				require.Equal(t, guid, entry.Extensions["guid"], "Router-Guid equals extensions.guid")
			})
		}
	}

	t.Run("control: OPTIONS keeps its 204 with no body", func(t *testing.T) {
		relayed := relay.relayed
		reply := send(http.MethodOptions, "/x/y", "")
		require.Equal(t, http.StatusNoContent, reply.status)
		require.Empty(t, reply.body)
		require.Equal(t, relayed, relay.relayed)
	})

	t.Run("control: a GET on the health path is the health check", func(t *testing.T) {
		reply := send(http.MethodGet, common.DEFAULT_HEALTH_PATH, "")
		require.Equal(t, http.StatusOK, reply.status, "body: %s", reply.body)
	})

	t.Run("control: a POST is served at any path", func(t *testing.T) {
		relayed := relay.relayed
		reply := send(http.MethodPost, "/x/y", request)
		require.Equal(t, http.StatusOK, reply.status, "body: %s", reply.body)
		require.Equal(t, relayed+1, relay.relayed, "the POST reaches the relay")
	})
}

// The other listeners keep the shared DELETE answer until their own refusal is built: the base
// listener answers a DELETE with 204 before any route runs, unless the listener routes it itself.
func TestCreateAndSetupBaseAppListener_RoutesDeleteOnlyWhenAsked(t *testing.T) {
	for _, routesDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("routesDelete=%v", routesDelete), func(t *testing.T) {
			app := createAndSetupBaseAppListener(common.ConsumerCmdFlags{}, "/health", alwaysHealthyReporter{}, false, routesDelete)
			app.Delete("/*", func(c *fiber.Ctx) error { return c.Status(http.StatusTeapot).SendString("route") })
			response, err := app.Test(httptest.NewRequest(http.MethodDelete, "/x/y", nil))
			require.NoError(t, err)
			defer response.Body.Close()
			if routesDelete {
				require.Equal(t, http.StatusTeapot, response.StatusCode, "the listener's own route answers")
				return
			}
			require.Equal(t, http.StatusNoContent, response.StatusCode, "the shared 204, as before")
		})
	}
}
