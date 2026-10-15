package common

import (
	"testing"

	"github.com/magma-Devs/smart-router/utils/testcert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// cacert-pem is a trust anchor: the roots are loaded and verification stays on. It used to
// be loaded and then switched off in the same breath (InsecureSkipVerify after RootCAs),
// which left the setting with no effect beyond its log line (MAG-4278 #13).
func TestAuthConfigTLSConfig_CACertificateIsATrustAnchor(t *testing.T) {
	_, caPath, keyPath := testcert.SelfSigned(t)

	withCA := (&AuthConfig{CaCert: caPath}).TLSConfig()
	require.NotNil(t, withCA.RootCAs, "the configured CA is the root set")
	require.False(t, withCA.InsecureSkipVerify, "loading a CA must not switch verification off")

	insecureToo := (&AuthConfig{CaCert: caPath, AllowInsecure: true}).TLSConfig()
	require.NotNil(t, insecureToo.RootCAs)
	require.True(t, insecureToo.InsecureSkipVerify, "allow-insecure is the one setting that disables verification")

	missing := (&AuthConfig{CaCert: caPath + ".missing"}).TLSConfig()
	require.Nil(t, missing.RootCAs, "an unreadable CA file falls back to the system roots")
	require.False(t, missing.InsecureSkipVerify)

	clientCert := (&AuthConfig{CertPem: caPath, KeyPem: keyPath}).TLSConfig()
	require.Len(t, clientCert.Certificates, 1, "cert-pem/key-pem is the client certificate to present")
	require.Nil(t, clientCert.RootCAs)

	var none *AuthConfig
	require.Equal(t, (&AuthConfig{}).TLSConfig(), none.TLSConfig(), "a nil auth-config is the zero configuration")
}

// cert-pem/key-pem is the client certificate with or without cacert-pem: the CA verifies
// the server, the certificate identifies the client, and together they are the mutual-TLS
// pair a private CA implies. The certificate used to be loaded only when no CA was set, so
// that pair presented none.
func TestAuthConfigTLSConfig_ClientCertificateBesideACA(t *testing.T) {
	_, caPath, _ := testcert.SelfSigned(t)
	_, clientCertPath, clientKeyPath := testcert.SelfSigned(t)

	both := (&AuthConfig{CaCert: caPath, CertPem: clientCertPath, KeyPem: clientKeyPath}).TLSConfig()
	require.NotNil(t, both.RootCAs, "the configured CA is the root set")
	require.Len(t, both.Certificates, 1, "the client certificate is presented beside the CA")
	require.False(t, both.InsecureSkipVerify)
}

// Every dialer shares the transport, the receive-size cap and the auth-headers, in that
// order, and keeps its own channel options at the end.
func TestNodeUrlGRPCDialOptions_SharedOptionsThenTheCallersOwn(t *testing.T) {
	transport := grpc.WithTransportCredentials(insecure.NewCredentials())
	bare := &NodeUrl{Url: "grpc://127.0.0.1:1"}
	require.Len(t, bare.GRPCDialOptions(transport), 2, "transport and receive-size cap")
	require.Len(t, bare.GRPCDialOptions(transport, grpc.WithBlock(), grpc.WithIdleTimeout(0)), 4, "the caller's options are appended")

	withAuth := &NodeUrl{Url: "grpc://127.0.0.1:1", AuthConfig: AuthConfig{AuthHeaders: map[string]string{"Authorization": "Bearer x"}}}
	require.Len(t, withAuth.GRPCDialOptions(transport, grpc.WithBlock()), 3+len(withAuth.GrpcAuthDialOptions()),
		"the MAG-2218 auth attachment rides along on every dial")
	require.NotEmpty(t, withAuth.GrpcAuthDialOptions(), "the fixture configures auth-headers")
}
