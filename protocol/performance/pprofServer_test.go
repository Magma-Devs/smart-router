package performance

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPprofApp_ServesGoroutineLeakProfile(t *testing.T) {
	app := newPprofApp()

	// The dedicated route must answer with the profile, not the fiber pprof
	// middleware's redirect to the index.
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutineleak?debug=1", nil))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(body), "goroutineleak profile:"), "unexpected body: %.80s", body)

	// The standard profiles still go through the middleware.
	resp, err = app.Test(httptest.NewRequest(http.MethodGet, "/debug/pprof/goroutine?debug=1", nil))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
