package utils_test

import (
	"testing"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/stretchr/testify/require"
)

func TestScrubSecrets(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		mustNotHave []string
		mustHave    []string // structure that should survive scrubbing
	}{
		{
			name:        "url query api key",
			in:          `provider https://node.example.com/rpc?api_key=SECRETKEY12345 ok`,
			mustNotHave: []string{"SECRETKEY12345"},
			mustHave:    []string{"node.example.com", "api_key=[REDACTED]"},
		},
		{
			name:        "url path token",
			in:          `https://lb.drpc.live/ethereum/AbCdEf0123456789AbCdEf0123456789`,
			mustNotHave: []string{"AbCdEf0123456789AbCdEf0123456789"},
			mustHave:    []string{"lb.drpc.live", "[REDACTED]"},
		},
		{
			name:        "ipv4 scrubbed, method kept",
			in:          `{"method":"eth_call","client":"192.168.1.42"}`,
			mustNotHave: []string{"192.168.1.42"},
			mustHave:    []string{"eth_call", "[REDACTED]"},
		},
		{
			name:        "no secrets left intact",
			in:          `{"method":"eth_blockNumber","params":[]}`,
			mustNotHave: []string{"[REDACTED]"},
			mustHave:    []string{"eth_blockNumber"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := utils.ScrubSecrets(tc.in)
			for _, s := range tc.mustNotHave {
				require.NotContains(t, out, s, "secret/IP should be scrubbed")
			}
			for _, s := range tc.mustHave {
				require.Contains(t, out, s, "readable structure should survive")
			}
		})
	}
}

func TestRedactPayloadHonorsUnsafeFlag(t *testing.T) {
	t.Cleanup(func() { utils.SetLogUnsafePayloads(false) })

	const body = `{"method":"eth_call","key":"https://x/v2/SECRET_TOKEN_VALUE_123","ip":"10.0.0.1"}`

	// Safe (default): keys/IPs scrubbed, structure readable.
	utils.SetLogUnsafePayloads(false)
	redacted := utils.RedactPayload(body)
	require.NotContains(t, redacted, "SECRET_TOKEN_VALUE_123")
	require.NotContains(t, redacted, "10.0.0.1")
	require.Contains(t, redacted, "eth_call")

	// Empty stays empty.
	require.Equal(t, "", utils.RedactPayload(""))

	// Unsafe: verbatim passthrough.
	utils.SetLogUnsafePayloads(true)
	require.Equal(t, body, utils.RedactPayload(body))

	// any-typed variant follows the same flag.
	require.Equal(t, body, utils.RedactPayloadAny(body))
	utils.SetLogUnsafePayloads(false)
	require.NotContains(t, utils.RedactPayloadAny(body).(string), "SECRET_TOKEN_VALUE_123")
}
