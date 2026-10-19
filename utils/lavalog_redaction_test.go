package utils

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"

	zerolog "github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLog swaps the global zerolog sink for a buffer, runs fn, and returns
// everything written. lavalog writes through a package-level singleton, so the
// previous logger and level are restored before returning.
func captureLog(t *testing.T, level zerolog.Level, fn func()) string {
	t.Helper()
	prevLogger := zerologlog.Logger
	prevLevel := defaultGlobalLogLevel
	t.Cleanup(func() {
		zerologlog.Logger = prevLogger
		defaultGlobalLogLevel = prevLevel
	})

	var buf bytes.Buffer
	defaultGlobalLogLevel = level
	zerologlog.Logger = zerolog.New(&buf).Level(level)
	fn()
	return buf.String()
}

const (
	testSecretKey           = "AbC123SecretKey"
	testSecretURL           = "https://eth-mainnet.example.com/v2/" + testSecretKey
	testSchemeLessSecretURL = "eth-mainnet.example.com/v2/" + testSecretKey
)

// MAG-3330: ~87 call sites log a raw node-url. Redacting inside LavaFormatLog is
// what covers all of them at once, so the wiring — not just RedactSecrets — is
// what this asserts.
func TestLavaFormatLog_RedactsURLInAttribute(t *testing.T) {
	out := captureLog(t, zerolog.TraceLevel, func() {
		LavaFormatInfo("created direct RPC connection", LogAttr("url", testSecretURL))
	})

	assert.NotContains(t, out, testSecretKey)
	assert.Contains(t, out, "eth-mainnet.example.com", "the host stays, so the line is still actionable")
	assert.Contains(t, out, "created direct RPC connection")
}

func TestLavaFormatLog_RedactsSchemeLessURLInAttribute(t *testing.T) {
	out := captureLog(t, zerolog.TraceLevel, func() {
		LavaFormatInfo("created direct RPC connection", LogAttr("url", testSchemeLessSecretURL))
	})

	assert.NotContains(t, out, testSecretKey)
	assert.Contains(t, out, "eth-mainnet.example.com")
}

func TestLavaFormatLog_RedactsURLInWrappedError(t *testing.T) {
	transportErr := fmt.Errorf("http request failed: %w", &url.Error{
		Op:  "Post",
		URL: testSecretURL,
		Err: errors.New("connection reset by peer"),
	})
	require.Contains(t, transportErr.Error(), testSecretKey, "precondition")

	var returned error
	out := captureLog(t, zerolog.TraceLevel, func() {
		returned = LavaFormatError("relay failed", transportErr)
	})

	assert.NotContains(t, out, testSecretKey, "log sink")
	// The returned error is what propagates toward the client envelope.
	assert.NotContains(t, returned.Error(), testSecretKey, "returned error message")
	assert.Contains(t, returned.Error(), "connection reset by peer")
	// The cause is untouched, so errors.Is/As still work through the wrap.
	var urlErr *url.Error
	assert.True(t, errors.As(returned, &urlErr))
}

func TestLavaFormatLog_RedactsURLAcrossEveryLevel(t *testing.T) {
	levels := map[string]func(){
		"info":    func() { LavaFormatInfo("m", LogAttr("url", testSecretURL)) },
		"debug":   func() { LavaFormatDebug("m", LogAttr("url", testSecretURL)) },
		"trace":   func() { LavaFormatTrace("m", LogAttr("url", testSecretURL)) },
		"warning": func() { LavaFormatWarning("m", nil, LogAttr("url", testSecretURL)) },
		"error":   func() { LavaFormatError("m", nil, LogAttr("url", testSecretURL)) },
	}
	for name, emit := range levels {
		t.Run(name, func(t *testing.T) {
			out := captureLog(t, zerolog.TraceLevel, emit)
			require.NotEmpty(t, strings.TrimSpace(out), "the level must actually emit")
			assert.NotContains(t, out, testSecretKey)
		})
	}
}

// Redaction must not cost the rest of the line: an attribute that follows a url
// in the same call has to survive intact.
func TestLavaFormatLog_KeepsAttributesAfterAURL(t *testing.T) {
	out := captureLog(t, zerolog.TraceLevel, func() {
		LavaFormatInfo("relay",
			LogAttr("url", testSecretURL),
			LogAttr("method", "eth_call"),
			LogAttr("chainId", "ETH1"),
		)
	})

	assert.NotContains(t, out, testSecretKey)
	assert.Contains(t, out, "eth_call")
	assert.Contains(t, out, "ETH1")
}

// A caller that stops writing a line must not change the error its client reads.
func TestLavaFormatErrorSilent_ReturnsLavaFormatErrorsErrorAndWritesNothing(t *testing.T) {
	cause := fmt.Errorf("post %s: %w", testSecretURL, errors.New("connection refused"))
	cases := map[string]struct {
		err   error
		attrs []Attribute
	}{
		"cause and attributes": {cause, []Attribute{LogAttr("GUID", "7003"), LogAttr("endpoint", "ETH1jsonrpc")}},
		"cause only":           {cause, nil},
		"attributes only":      {nil, []Attribute{LogAttr("url", testSecretURL)}},
		"neither":              {nil, nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var loud, silent error
			logged := captureLog(t, zerolog.TraceLevel, func() {
				loud = LavaFormatError("failed processing responses from RPC endpoints", tc.err, tc.attrs...)
			})
			written := captureLog(t, zerolog.TraceLevel, func() {
				silent = LavaFormatErrorSilent("failed processing responses from RPC endpoints", tc.err, tc.attrs...)
			})
			require.NotEmpty(t, strings.TrimSpace(logged), "LavaFormatError writes the line")
			assert.Empty(t, written)
			assert.Equal(t, loud.Error(), silent.Error())
			assert.NotContains(t, silent.Error(), testSecretKey)
			if tc.err != nil {
				assert.ErrorIs(t, silent, tc.err, "the cause stays reachable")
			}
		})
	}
}
