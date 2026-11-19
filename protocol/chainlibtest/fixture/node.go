// Package fixture serves the mock upstream nodes and builds the mock specs chainlib tests run
// against. Only tests import it.
//
// It does not import chainlib, so chainlib's own tests (package chainlib) can use it without an
// import cycle; the helpers that need chainlib types live in the parent chainlibtest package.
package fixture

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
)

// StartNode serves a mock node for apiInterface and returns the node URLs an endpoint lists for
// it, one per addon and extension combination, and a function that stops the node. A gRPC node
// answers the test block service with httpHandler; any other interface gets an HTTP server on
// httpHandler and a WebSocket server on wsHandler. A nil httpHandler answers 200, a nil wsHandler
// echoes every message.
func StartNode(apiInterface string, httpHandler, wsHandler http.HandlerFunc, addons, extensions []string) (nodeUrls []common.NodeUrl, closeNode func(), err error) {
	if httpHandler == nil {
		httpHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	}

	if wsHandler == nil {
		wsHandler = genericWebSocketHandler()
	}

	if apiInterface == spectypes.APIInterfaceGrpc {
		// Start a new gRPC server using the buffered connection
		grpcServer := grpc.NewServer()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, nil, err
		}
		nodeUrls = append(nodeUrls, common.NodeUrl{Url: lis.Addr().String(), Addons: addons})
		allCombinations := generateCombinations(extensions)
		for _, extensionsList := range allCombinations {
			nodeUrls = append(nodeUrls, common.NodeUrl{Url: lis.Addr().String(), Addons: append(addons, extensionsList...)})
		}
		go func() {
			service := mockBlockServiceImpl{serverCallback: httpHandler}
			grpcServer.RegisterService(&BlockServiceDesc, service)
			reflection.Register(grpcServer)
			// Serve requests on the buffered connection
			if err := grpcServer.Serve(lis); err != nil {
				return
			}
		}()
		time.Sleep(10 * time.Millisecond)
		return nodeUrls, grpcServer.Stop, nil
	}

	mockWebSocketServer := httptest.NewServer(wsHandler)
	wsUrl := "ws" + strings.TrimPrefix(mockWebSocketServer.URL, "http")
	mockHttpServer := httptest.NewServer(httpHandler)
	closeNode = func() {
		mockHttpServer.Close()
		mockWebSocketServer.Close()
	}
	nodeUrls = append(nodeUrls, common.NodeUrl{Url: mockHttpServer.URL, Addons: addons})
	// Only add WebSocket URLs for API interfaces that support WebSocket (JsonRPC and TendermintRPC)
	// REST API only supports HTTP/HTTPS, not WebSocket
	supportsWebSocket := apiInterface == spectypes.APIInterfaceJsonRPC || apiInterface == spectypes.APIInterfaceTendermintRPC
	if len(extensions) > 0 {
		nodeUrls = append(nodeUrls, common.NodeUrl{Url: mockHttpServer.URL, Addons: extensions})
		if supportsWebSocket {
			nodeUrls = append(nodeUrls, common.NodeUrl{Url: wsUrl, Addons: extensions})
		}
	}
	if supportsWebSocket {
		nodeUrls = append(nodeUrls, common.NodeUrl{Url: wsUrl, Addons: nil})
	}
	return nodeUrls, closeNode, nil
}

func generateCombinations(arr []string) [][]string {
	if len(arr) == 0 {
		return [][]string{{}}
	}

	first, rest := arr[0], arr[1:]
	combinationsWithoutFirst := generateCombinations(rest)
	var combinationsWithFirst [][]string

	for _, c := range combinationsWithoutFirst {
		combinationsWithFirst = append(combinationsWithFirst, append(c, first))
	}

	return append(combinationsWithoutFirst, combinationsWithFirst...)
}

func genericWebSocketHandler() http.HandlerFunc {
	upGrader := websocket.Upgrader{}

	// Create a simple websocket server that mocks the node
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upGrader.Upgrade(w, r, nil)
		if err != nil {
			fmt.Println(err)
			panic("got error in upgrader")
		}
		defer conn.Close()

		for {
			// Read the request
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				// Don't panic here, just return to close the connection gracefully
				return
			}
			fmt.Println("got ws message", string(message), messageType)
			conn.WriteMessage(messageType, message)
			fmt.Println("writing ws message", string(message), messageType)
		}
	}
}
