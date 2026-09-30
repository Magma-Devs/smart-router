package chainlib

import (
	"net/http/httptest"
	"strings"
	"testing"
	"unsafe"

	"github.com/gofiber/fiber/v2"
	"github.com/magma-Devs/smart-router/protocol/metrics"
	"github.com/stretchr/testify/require"
)

// sharesMemory reports whether two strings start at the same byte, which is how a zero-copy view
// and the buffer it points into look.
func sharesMemory(a, b string) bool {
	return len(a) > 0 && len(b) > 0 && unsafe.StringData(a) == unsafe.StringData(b)
}

// TestDetachedReqHeaders_OwnsEveryString pins the listener boundary of MAG-3881: fiber hands header
// strings back zero-copy, and every one the router keeps must be its own. The comparison runs inside
// the handler, while the request buffer is still live, and is asserted after.
func TestDetachedReqHeaders_OwnsEveryString(t *testing.T) {
	var equal, shared, missing []string
	var viewsShared int
	app := fiber.New()
	app.Post("/", func(c *fiber.Ctx) error {
		detached := detachedReqHeaders(c)
		for name, values := range c.GetReqHeaders() {
			if len(detached[name]) != len(values) {
				missing = append(missing, name)
				continue
			}
			for i, value := range values {
				if detached[name][i] == value {
					equal = append(equal, name)
				}
				if sharesMemory(detached[name][i], value) {
					shared = append(shared, name)
				}
			}
		}
		// The control: two zero-copy reads of one header share memory, so the check above can see it.
		if sharesMemory(c.GetReqHeaders()["Lava-Select-Provider"][0], c.Get("Lava-Select-Provider")) {
			viewsShared++
		}
		return nil
	})
	req := httptest.NewRequest("POST", "/", strings.NewReader("{}"))
	req.Header.Set("Lava-Select-Provider", "ethprimaryprovider2")
	req.Header.Set("X-Request-Id", "tests.simulator.simulator_production_tests")
	_, err := app.Test(req)
	require.NoError(t, err)

	require.Equal(t, 1, viewsShared, "control failed: two zero-copy reads of one header were expected to share memory, without which sharesMemory cannot see aliasing")
	require.Empty(t, missing, "these headers were dropped, or lost a value, in the detached copy")
	require.Contains(t, equal, "Lava-Select-Provider")
	require.Contains(t, equal, "X-Request-Id")
	require.Empty(t, shared, "these headers still point into the request buffer")
}

// TestWebSocketLimiterLocals_UserAgentIsAnOwnString covers the User-Agent the connection limiter
// stores before the websocket handler runs. CanOpenConnection reads it inside that handler, and with
// metrics off nothing overwrites it with a copy, so the limiter's own write has to be owned.
func TestWebSocketLimiterLocals_UserAgentIsAnOwnString(t *testing.T) {
	var stored string
	var shared bool
	limiter := &WebsocketConnectionLimiter{ipToNumberOfActiveConnections: map[string]int64{}}
	app := fiber.New()
	app.Get("/ws", func(c *fiber.Ctx) error {
		limiter.HandleFiberRateLimitFlags(c)
		stored, _ = c.Locals(fiber.HeaderUserAgent).(string)
		shared = sharesMemory(stored, c.Get(fiber.HeaderUserAgent))
		return nil
	})
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Header.Set(fiber.HeaderUserAgent, "tests.simulator.agent")
	_, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, "tests.simulator.agent", stored)
	require.False(t, shared, "the stored User-Agent still points into the request buffer")
}

// TestWebSocketLocals_AreOwnStrings covers the three header values stored for the websocket
// handler. Its metrics goroutines read them, and can do so after fasthttp has recycled the request
// buffer.
func TestWebSocketLocals_AreOwnStrings(t *testing.T) {
	var shared []string
	var read int
	handler := constructFiberCallbackWithHeaderAndParameterExtraction(func(c *fiber.Ctx) error {
		for _, key := range []string{metrics.RefererHeaderKey, metrics.UserAgentHeaderKey, metrics.OriginHeaderKey} {
			stored, _ := c.Locals(key).(string)
			if stored != "" {
				read++
			}
			if sharesMemory(stored, c.Get(key)) {
				shared = append(shared, key)
			}
		}
		return nil
	}, true)
	app := fiber.New()
	app.Get("/ws", handler)
	req := httptest.NewRequest("GET", "/ws", nil)
	req.Header.Set(metrics.RefererHeaderKey, "https://referer.example")
	req.Header.Set(metrics.UserAgentHeaderKey, "tests.simulator.agent")
	req.Header.Set(metrics.OriginHeaderKey, "https://origin.example")
	_, err := app.Test(req)
	require.NoError(t, err)
	require.Equal(t, 3, read)
	require.Empty(t, shared)
}
