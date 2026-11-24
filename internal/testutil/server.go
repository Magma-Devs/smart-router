package testutil

import (
	"context"
	"time"

	"github.com/magma-Devs/smart-router/utils"
)

// WaitForServer dials, with dial, until a server a test binary started answers. Each attempt
// gets its own five-second context, cancelled before the next, and a failed attempt is
// followed by a short pause. It returns the last attempt's error if the server has not
// answered within a minute.
func WaitForServer(dial func(ctx context.Context) error) error {
	deadline := time.Now().Add(time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := dial(ctx)
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		utils.FormatDebug("waiting for the test server to answer", utils.LogAttr("error", err))
		time.Sleep(10 * time.Millisecond)
	}
}
