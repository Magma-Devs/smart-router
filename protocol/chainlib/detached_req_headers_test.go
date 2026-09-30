package chainlib

import (
	"net/http/httptest"
	"strings"
	"testing"
	"unsafe"

	"github.com/gofiber/fiber/v2"
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
