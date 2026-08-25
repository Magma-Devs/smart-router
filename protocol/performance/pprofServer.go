package performance

import (
	"fmt"
	"net/http/pprof"

	"github.com/gofiber/fiber/v2"
	fiberpprof "github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/valyala/fasthttp/fasthttpadaptor"

	"github.com/magma-Devs/smart-router/utils"
)

const (
	PprofAddressFlagName = "pprof-address"
)

// goroutineLeakProfile is the Go 1.27 runtime profile of goroutines blocked on
// a primitive that can never be released. The fiber pprof middleware routes a
// fixed list of profile names and redirects anything else, so this one gets
// its own handler on the standard net/http/pprof path.
const goroutineLeakProfile = "goroutineleak"

func StartPprofServer(addr string) error {
	// Set up the Fiber app
	app := fiber.New()

	app.Get("/debug/pprof/"+goroutineLeakProfile, func(c *fiber.Ctx) error {
		fasthttpadaptor.NewFastHTTPHandler(pprof.Handler(goroutineLeakProfile))(c.Context())
		return nil
	})

	// Let the fiber HTTP server use pprof
	app.Use(fiberpprof.New())

	// Start the HTTP server in a goroutine
	go func() {
		fmt.Printf("Starting server on %s\n", addr)
		if err := app.Listen(addr); err != nil {
			fmt.Printf("Error starting pprof HTTP server: %s\n", err)
		}
	}()

	utils.LavaFormatInfo("start pprof HTTP server", utils.Attribute{Key: "IPAddress", Value: addr})

	return nil
}
