//go:build enterprise

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Magma-Devs/smart-router/licensing"
	"github.com/Magma-Devs/smart-router/protocol/rpcsmartrouter"
)

// testKey is an ephemeral Ed25519 keypair plus its registered key_id. Mirrors
// the licensing/license_test.go pattern; inlined here because the licensing
// helpers aren't exported (advisor's call: inline until a third user appears).
type testKey struct {
	keyID string
	pub   ed25519.PublicKey
	priv  ed25519.PrivateKey
}

// registerTestKey generates a keypair, registers the public half in
// licensing.PublicKeys under a unique nonce-suffixed key_id, and arranges
// cleanup. Tests using this helper MUST NOT call t.Parallel() — PublicKeys
// is a shared map.
func registerTestKey(t *testing.T) testKey {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err, "generate ed25519 key")

	var nonce [4]byte
	_, err = rand.Read(nonce[:])
	require.NoError(t, err, "read random nonce")

	keyID := fmt.Sprintf("key_sprint3_test_%x", nonce)
	_, exists := licensing.PublicKeys[keyID]
	require.False(t, exists, "key id collision: %q already registered", keyID)
	licensing.PublicKeys[keyID] = pub
	t.Cleanup(func() { delete(licensing.PublicKeys, keyID) })

	return testKey{keyID: keyID, pub: pub, priv: priv}
}

// signLicense JSON-encodes lic, signs it with key.priv, and returns the
// base64url envelope. Overwrites lic.KeyID with key.keyID so callers don't
// have to wire it themselves.
func signLicense(t *testing.T, key testKey, lic *licensing.License) string {
	t.Helper()
	lic.KeyID = key.keyID
	payload, err := json.Marshal(lic)
	require.NoError(t, err, "marshal license")
	sig := ed25519.Sign(key.priv, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func newTestLicense(licenseID string, expiresAt time.Time) *licensing.License {
	return &licensing.License{
		LicenseID:  licenseID,
		CustomerID: "cust_test",
		ExpiresAt:  expiresAt,
		MaxNodes:   0,
		IssuedAt:   time.Now().Add(-1 * time.Hour),
	}
}

func TestValidateAndActivateLicense_Valid_PromotesActiveConfig(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense("lic_valid_001", time.Now().Add(180*24*time.Hour))
	licenseKey := signLicense(t, key, lic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // stops the expiryWatcher goroutine started inside validateAndActivateLicense

	require.NoError(t, validateAndActivateLicense(ctx, licenseKey))

	got := rpcsmartrouter.ActiveConfig()
	assert.Equal(t, "enterprise", got.Edition(),
		"valid license must promote activeConfig to enterprise")
	require.NotNil(t, got.License())
	assert.Equal(t, "lic_valid_001", got.License().LicenseID)
}

func TestValidateAndActivateLicense_GracePeriod_PromotesActiveConfig(t *testing.T) {
	key := registerTestKey(t)
	// Expired 5 days ago — within the 14-day GracePeriod (constant in licensing).
	lic := newTestLicense("lic_grace_001", time.Now().Add(-5*24*time.Hour))
	licenseKey := signLicense(t, key, lic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, validateAndActivateLicense(ctx, licenseKey))

	got := rpcsmartrouter.ActiveConfig()
	assert.Equal(t, "enterprise", got.Edition(),
		"grace-period license must still promote (router stays operational)")
	require.NotNil(t, got.License())
	assert.Equal(t, "lic_grace_001", got.License().LicenseID)
}

// FATAL paths are tested via resolveLicense (pure function — no os.Exit).
// validateAndActivateLicense's role for those paths is just "format the
// LavaFormatFatal call from the decision and call it" — visual review of
// that one-line dispatch is sufficient since resolveLicense's table covers
// every status × error combination that produces a fatal decision.

func TestResolveLicense_Valid(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense("lic_valid_test", time.Now().Add(180*24*time.Hour))
	licenseKey := signLicense(t, key, lic)

	d := resolveLicense(licenseKey)
	assert.False(t, d.shouldFatal(), "valid license must not produce fatal decision")
	require.NotNil(t, d.license)
	assert.Equal(t, "lic_valid_test", d.license.LicenseID)
	assert.Equal(t, licensing.LicenseStatusValid, d.status)
	assert.NoError(t, d.err)
}

func TestResolveLicense_GracePeriod(t *testing.T) {
	key := registerTestKey(t)
	// Expired 5 days ago — within the 14-day GracePeriod.
	lic := newTestLicense("lic_grace_test", time.Now().Add(-5*24*time.Hour))
	licenseKey := signLicense(t, key, lic)

	d := resolveLicense(licenseKey)
	assert.False(t, d.shouldFatal(), "grace-period license must not produce fatal decision")
	require.NotNil(t, d.license)
	assert.Equal(t, "lic_grace_test", d.license.LicenseID)
	assert.Equal(t, licensing.LicenseStatusGracePeriod, d.status)
}

func TestResolveLicense_Expired_PastGrace_ProducesFatalDecision(t *testing.T) {
	key := registerTestKey(t)
	// Expired 30 days ago — past the 14-day grace period.
	lic := newTestLicense("lic_expired_test", time.Now().Add(-30*24*time.Hour))
	licenseKey := signLicense(t, key, lic)

	d := resolveLicense(licenseKey)
	require.True(t, d.shouldFatal(), "past-grace license must produce fatal decision")
	assert.Equal(t, licensing.LicenseStatusExpired, d.status)
	assert.Contains(t, d.fatalMsg, "license expired on")
	assert.Contains(t, d.fatalMsg, "grace period ended")
	assert.Contains(t, d.fatalMsg, "re-issue and rebuild")
	require.NotNil(t, d.license, "expired license is still parsed; the *License is needed for fatal log attributes")
	assert.Equal(t, "lic_expired_test", d.license.LicenseID)
}

func TestResolveLicense_BadSignature_ProducesFatalDecision(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense("lic_badsig_test", time.Now().Add(30*24*time.Hour))
	licenseKey := signLicense(t, key, lic)

	// Tamper with the payload so the signature no longer verifies.
	tampered := licenseKey[:len(licenseKey)-4] + "XXXX"

	d := resolveLicense(tampered)
	require.True(t, d.shouldFatal(), "bad-signature license must produce fatal decision")
	assert.Contains(t, d.fatalMsg, "license validation failed")
	require.Error(t, d.err)
}

func TestResolveLicense_MalformedEnvelope_ProducesFatalDecision(t *testing.T) {
	d := resolveLicense("not-a-valid-license-string")
	require.True(t, d.shouldFatal(), "malformed envelope must produce fatal decision")
	assert.Contains(t, d.fatalMsg, "license validation failed")
	require.Error(t, d.err)
}

func TestResolveLicense_UnknownKeyID_ProducesFatalDecision(t *testing.T) {
	// Mint a license signed by a key that's NEVER registered in licensing.PublicKeys.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_ = pub // public half is intentionally NOT registered
	lic := newTestLicense("lic_unknown_key", time.Now().Add(30*24*time.Hour))
	lic.KeyID = "key_never_registered_xyz"
	payload, err := json.Marshal(lic)
	require.NoError(t, err)
	sig := ed25519.Sign(priv, payload)
	licenseKey := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)

	d := resolveLicense(licenseKey)
	require.True(t, d.shouldFatal(), "unknown key_id must produce fatal decision")
	assert.Contains(t, d.fatalMsg, "license validation failed")
	require.Error(t, d.err)
	assert.Contains(t, d.err.Error(), "unknown signing key")
}

func TestWatcherCadence(t *testing.T) {
	cases := []struct {
		days int
		want time.Duration
	}{
		{-3, time.Hour},      // grace period — already past expiry
		{-1, time.Hour},      // grace period
		{0, time.Hour},       // last 7 days — boundary
		{6, time.Hour},       // last 7 days
		{7, 24 * time.Hour},  // last 30 days — boundary
		{29, 24 * time.Hour}, // last 30 days
		{30, 0},              // outside warning window — boundary
		{365, 0},             // outside warning window
	}
	for i, tc := range cases {
		t.Run(fmt.Sprintf("days=%d", tc.days), func(t *testing.T) {
			got := watcherCadence(tc.days)
			assert.Equal(t, tc.want, got, "watcherCadence(%d) tc #%d, i #%d", tc.days, i, i)
		})
	}
}

func TestExpiryWatcher_ExitsOnCtxCancel(t *testing.T) {
	// License far in the future — watcher polls daily, no warning logs fire.
	lic := newTestLicense("lic_watcher_test", time.Now().Add(365*24*time.Hour))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		expiryWatcher(ctx, lic)
		close(done)
	}()

	// Cancel immediately and verify the goroutine returns within a tight bound.
	// expiryWatcher has a 24h sleep between iterations when outside the warning
	// window, but the select{} also watches ctx.Done(), so cancellation must
	// short-circuit the sleep.
	cancel()

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("expiryWatcher did not exit within 2s of ctx.Cancel()")
	}
}

func TestInstallLicenseCheck_ChainsExistingPreRunE(t *testing.T) {
	cmd := &cobra.Command{Use: "test"}
	var existingFired bool
	cmd.PreRunE = func(*cobra.Command, []string) error {
		existingFired = true
		return nil
	}

	// Mint a valid license so the new PreRunE doesn't bail out.
	key := registerTestKey(t)
	lic := newTestLicense("lic_chain_test", time.Now().Add(30*24*time.Hour))
	licenseKey := signLicense(t, key, lic)
	t.Cleanup(func() {})

	installLicenseCheck(cmd)

	// Manually invoke via a context with our test license. Since
	// installLicenseCheck calls licensing.EmbeddedLicense() inside its closure,
	// we exercise the *chaining* (existing PreRunE still fires) by directly
	// invoking validateAndActivateLicense first to leave activeConfig in a
	// known state, then calling cmd.PreRunE which will re-validate the embedded
	// production license.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, validateAndActivateLicense(ctx, licenseKey),
		"sanity: pre-arming activeConfig for the chained-PreRunE test")

	// The cmd.PreRunE we're testing reads licensing.EmbeddedLicense() (the
	// production license) — that's expected to validate cleanly in this build.
	cmd.SetContext(ctx)
	err := cmd.PreRunE(cmd, nil)
	require.NoError(t, err, "chained PreRunE must not return an error against the production embedded license")
	assert.True(t, existingFired, "chained PreRunE must invoke the previously-installed PreRunE")
}
