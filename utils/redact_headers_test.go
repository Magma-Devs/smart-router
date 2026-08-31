package utils

import (
	"net/http"
	"testing"

	"google.golang.org/grpc/metadata"

	"github.com/stretchr/testify/assert"
)

func TestIsSensitiveHeader_FailsClosed(t *testing.T) {
	// Known-safe transport headers keep their values.
	for _, name := range []string{"Content-Type", "content-type", "  Accept  ", "User-Agent", "Host"} {
		assert.False(t, IsSensitiveHeader(name), "%q should be loggable", name)
	}

	// Credential-bearing names, and — the point of failing closed — the
	// operator-configured ones no deny-list could enumerate.
	for _, name := range []string{
		"Authorization", "authorization", "X-Api-Key", "x-lava-key", "Cookie",
		"Proxy-Authorization", "X-Auth-Token",
		"X-Vendor-Specific-Secret", "Some-Custom-Thing",
	} {
		assert.True(t, IsSensitiveHeader(name), "%q must be withheld", name)
	}
}

func TestRedactHeaders_HTTPHeader(t *testing.T) {
	// The shape of the leak: SetAuthHeaders puts the upstream credential on the
	// request, and the next log line printed the whole header set.
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("X-Api-Key", "AbC123SecretKey")
	h.Set("Authorization", "Bearer AbC123SecretKey")

	got := RedactHeaders(h)

	assert.NotContains(t, got, "AbC123SecretKey")
	// Names survive, so the line still says what was sent.
	assert.Contains(t, got, "X-Api-Key")
	assert.Contains(t, got, "Authorization")
	assert.Contains(t, got, "Content-Type:application/json")
}

func TestRedactHeaders_GRPCMetadata(t *testing.T) {
	// metadata.MD assigns to map[string][]string, so one helper covers it.
	md := metadata.New(map[string]string{
		"content-type": "application/grpc",
		"x-api-key":    "AbC123SecretKey",
	})

	got := RedactHeaders(md)

	assert.NotContains(t, got, "AbC123SecretKey")
	assert.Contains(t, got, "content-type:application/grpc")
	assert.Contains(t, got, "x-api-key:"+RedactedMark)
}

func TestRedactHeaderMap(t *testing.T) {
	got := RedactHeaderMap(map[string]string{
		"content-type":  "application/json",
		"authorization": "Bearer AbC123SecretKey",
	})

	assert.NotContains(t, got, "AbC123SecretKey")
	assert.Contains(t, got, "content-type:application/json")
	assert.Contains(t, got, "authorization:"+RedactedMark)
}

// Map iteration order is random; unsorted output would make log lines differ
// run to run and this test flake.
func TestRedactHeaders_DeterministicOrder(t *testing.T) {
	h := map[string][]string{
		"zeta": {"1"}, "alpha": {"2"}, "mu": {"3"}, "beta": {"4"}, "omega": {"5"},
	}
	first := RedactHeaders(h)
	for i := 0; i < 50; i++ {
		assert.Equal(t, first, RedactHeaders(h))
	}
	assert.Equal(t, "{alpha:[redacted], beta:[redacted], mu:[redacted], omega:[redacted], zeta:[redacted]}", first)
}

func TestRedactHeaders_Empty(t *testing.T) {
	assert.Equal(t, "{}", RedactHeaders(nil))
	assert.Equal(t, "{}", RedactHeaderMap(nil))
}
