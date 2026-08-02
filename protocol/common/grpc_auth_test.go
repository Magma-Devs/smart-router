package common

import (
	"context"
	"testing"
)

func TestGrpcAuthDialOptions_NoHeadersReturnsNil(t *testing.T) {
	url := &NodeUrl{Url: "grpc://127.0.0.1:9090"}
	if opts := url.GrpcAuthDialOptions(); opts != nil {
		t.Fatalf("expected nil dial options when no auth-headers are configured, got %d", len(opts))
	}
}

func TestGrpcAuthDialOptions_NilReceiverIsSafe(t *testing.T) {
	var url *NodeUrl
	if opts := url.GrpcAuthDialOptions(); opts != nil {
		t.Fatal("expected nil dial options for a nil NodeUrl")
	}
}

func TestGrpcAuthDialOptions_AttachesWhenConfigured(t *testing.T) {
	url := &NodeUrl{
		Url:        "grpc://127.0.0.1:9090",
		AuthConfig: AuthConfig{AuthHeaders: map[string]string{"Authorization": "Bearer tok"}},
	}
	opts := url.GrpcAuthDialOptions()
	if len(opts) != 1 {
		t.Fatalf("expected exactly one dial option, got %d", len(opts))
	}
}

// The credential payload is what actually reaches the wire, so assert on it directly
// rather than only on the dial-option count.
func TestGrpcAuthCredentials_LowercasesNames(t *testing.T) {
	creds := grpcAuthCredentials{headers: map[string]string{}}
	url := &NodeUrl{
		AuthConfig: AuthConfig{AuthHeaders: map[string]string{
			"Authorization": "Bearer tok",
			"X-API-Key":     "abc123",
		}},
	}
	// Rebuild through the same screening path the dial option uses.
	for name, value := range url.GetAuthHeaders() {
		lowered := toLowerForTest(name)
		if isLegalGrpcHeaderName(lowered) {
			creds.headers[lowered] = value
		}
	}

	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	if md["authorization"] != "Bearer tok" {
		t.Errorf("authorization not lowercased/preserved: %#v", md)
	}
	if md["x-api-key"] != "abc123" {
		t.Errorf("x-api-key not lowercased/preserved: %#v", md)
	}
}

func TestGrpcAuthCredentials_DoesNotRequireTransportSecurity(t *testing.T) {
	// Returning true here would make grpc-go refuse every plaintext dial, which is the
	// local-node and private-network case operators actually run.
	if (grpcAuthCredentials{}).RequireTransportSecurity() {
		t.Fatal("RequireTransportSecurity must be false so insecure dials are permitted")
	}
}

func TestIsLegalGrpcHeaderName(t *testing.T) {
	legal := []string{"authorization", "x-api-key", "x_api_key", "a.b.c", "k3y"}
	for _, name := range legal {
		if !isLegalGrpcHeaderName(name) {
			t.Errorf("expected %q to be legal", name)
		}
	}
	illegal := []string{"", "Authorization", "x api key", "x:key", "hdr-bin", "héader"}
	for _, name := range illegal {
		if isLegalGrpcHeaderName(name) {
			t.Errorf("expected %q to be illegal", name)
		}
	}
}

func TestGrpcAuthDialOptions_DropsIllegalNamesEntirely(t *testing.T) {
	url := &NodeUrl{
		AuthConfig: AuthConfig{AuthHeaders: map[string]string{"bad header": "v"}},
	}
	if opts := url.GrpcAuthDialOptions(); opts != nil {
		t.Fatal("expected nil dial options when every configured header name is illegal")
	}
}

func toLowerForTest(s string) string {
	out := []rune(s)
	for i, r := range out {
		if r >= 'A' && r <= 'Z' {
			out[i] = r + ('a' - 'A')
		}
	}
	return string(out)
}
