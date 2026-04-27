package licensing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// testKey is a per-test ephemeral keypair registered in the PublicKeys map.
type testKey struct {
	keyID string
	pub   ed25519.PublicKey
	priv  ed25519.PrivateKey
}

// registerTestKey generates an ephemeral Ed25519 keypair, registers the
// public half in the PublicKeys map under a unique key_id, and arranges for
// cleanup at test end. Tests must not call t.Parallel() on tests that use
// this helper because PublicKeys is a shared map.
func registerTestKey(t *testing.T) testKey {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	var nonce [4]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatalf("read random nonce: %v", err)
	}
	keyID := fmt.Sprintf("key_unit_test_%x", nonce)
	if _, exists := PublicKeys[keyID]; exists {
		t.Fatalf("key id collision: %q already registered", keyID)
	}
	PublicKeys[keyID] = pub
	t.Cleanup(func() { delete(PublicKeys, keyID) })
	return testKey{keyID: keyID, pub: pub, priv: priv}
}

// signLicense JSON-encodes lic, signs it with key.priv, and returns the
// base64url-encoded license envelope. lic.KeyID is overwritten with key.keyID
// so tests can declare the rest of the License struct without bookkeeping.
func signLicense(t *testing.T, key testKey, lic *License) string {
	t.Helper()
	lic.KeyID = key.keyID
	payload, err := json.Marshal(lic)
	if err != nil {
		t.Fatalf("marshal license: %v", err)
	}
	sig := ed25519.Sign(key.priv, payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func newTestLicense(expiresAt time.Time) *License {
	return &License{
		LicenseID:  "lic_test_001",
		CustomerID: "cust_test",
		ExpiresAt:  expiresAt,
		MaxNodes:   0,
		IssuedAt:   time.Now().Add(-1 * time.Hour),
	}
}

func TestValidate_Valid(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense(time.Now().Add(24 * time.Hour))
	licenseKey := signLicense(t, key, lic)

	got, status, err := Validate(licenseKey)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if status != LicenseStatusValid {
		t.Errorf("status = %v, want %v", status, LicenseStatusValid)
	}
	if got == nil {
		t.Fatal("got nil license")
	}
	if got.LicenseID != lic.LicenseID {
		t.Errorf("LicenseID = %q, want %q", got.LicenseID, lic.LicenseID)
	}
	if got.CustomerID != lic.CustomerID {
		t.Errorf("CustomerID = %q, want %q", got.CustomerID, lic.CustomerID)
	}
	if got.KeyID != key.keyID {
		t.Errorf("KeyID = %q, want %q", got.KeyID, key.keyID)
	}
}

func TestValidate_GracePeriod(t *testing.T) {
	key := registerTestKey(t)
	// Expired 7 days ago → still within the 14-day grace window.
	lic := newTestLicense(time.Now().Add(-7 * 24 * time.Hour))
	licenseKey := signLicense(t, key, lic)

	got, status, err := Validate(licenseKey)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if status != LicenseStatusGracePeriod {
		t.Errorf("status = %v, want %v", status, LicenseStatusGracePeriod)
	}
	if got == nil {
		t.Fatal("got nil license")
	}
}

func TestValidate_GracePeriod_AtBoundary(t *testing.T) {
	key := registerTestKey(t)
	// Expired exactly GracePeriod-1s ago → still in grace.
	lic := newTestLicense(time.Now().Add(-GracePeriod + time.Second))
	licenseKey := signLicense(t, key, lic)

	_, status, err := Validate(licenseKey)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if status != LicenseStatusGracePeriod {
		t.Errorf("status = %v, want %v", status, LicenseStatusGracePeriod)
	}
}

func TestValidate_Expired(t *testing.T) {
	key := registerTestKey(t)
	// Expired 15 days ago → past the 14-day grace window.
	lic := newTestLicense(time.Now().Add(-15 * 24 * time.Hour))
	licenseKey := signLicense(t, key, lic)

	got, status, err := Validate(licenseKey)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if status != LicenseStatusExpired {
		t.Errorf("status = %v, want %v", status, LicenseStatusExpired)
	}
	if got == nil {
		t.Fatal("expected non-nil license so callers can log expiry date")
	}
}

func TestValidate_InvalidSignature(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense(time.Now().Add(24 * time.Hour))
	licenseKey := signLicense(t, key, lic)

	// Flip one byte in the signature half.
	parts := strings.Split(licenseKey, ".")
	if len(parts) != 2 {
		t.Fatalf("malformed signed license: %d parts", len(parts))
	}
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}
	sigBytes[0] ^= 0xff
	tampered := parts[0] + "." + base64.RawURLEncoding.EncodeToString(sigBytes)

	_, status, err := Validate(tampered)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if status != LicenseStatusInvalid {
		t.Errorf("status = %v, want %v", status, LicenseStatusInvalid)
	}
	if !strings.Contains(err.Error(), "signature verification failed") {
		t.Errorf("error %q does not mention signature verification", err)
	}
}

func TestValidate_TamperedPayload(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense(time.Now().Add(24 * time.Hour))
	licenseKey := signLicense(t, key, lic)

	// Tamper with the payload but keep the original signature — sig verify must fail.
	parts := strings.Split(licenseKey, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var decoded License
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.CustomerID = "cust_attacker"
	tamperedPayload, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	tampered := base64.RawURLEncoding.EncodeToString(tamperedPayload) + "." + parts[1]

	_, status, err := Validate(tampered)
	if err == nil || status != LicenseStatusInvalid {
		t.Errorf("expected invalid+error, got status=%v err=%v", status, err)
	}
}

func TestValidate_MalformedBase64(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"payload not base64", "not!valid!base64.aGVsbG8"},
		{"signature not base64", "aGVsbG8.not!valid!base64"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, status, err := Validate(tc.key)
			if err == nil || status != LicenseStatusInvalid {
				t.Errorf("expected invalid+error, got status=%v err=%v", status, err)
			}
		})
	}
}

func TestValidate_MissingSeparator(t *testing.T) {
	cases := []string{
		"",
		"only-one-part",
		"too.many.parts.here",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, status, err := Validate(c)
			if err == nil || status != LicenseStatusInvalid {
				t.Errorf("expected invalid+error, got status=%v err=%v", status, err)
			}
			if !strings.Contains(err.Error(), "expected payload.signature") {
				t.Errorf("error %q does not mention expected format", err)
			}
		})
	}
}

func TestValidate_UnknownKeyID(t *testing.T) {
	// Create a keypair but DON'T register it in PublicKeys.
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = pub
	lic := newTestLicense(time.Now().Add(24 * time.Hour))
	lic.KeyID = "key_does_not_exist"
	payload, err := json.Marshal(lic)
	if err != nil {
		t.Fatal(err)
	}
	sig := ed25519.Sign(priv, payload)
	licenseKey := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)

	_, status, err := Validate(licenseKey)
	if err == nil || status != LicenseStatusInvalid {
		t.Fatalf("expected invalid+error, got status=%v err=%v", status, err)
	}
	if !strings.Contains(err.Error(), "unknown signing key") {
		t.Errorf("error %q does not mention unknown signing key", err)
	}
	if !strings.Contains(err.Error(), "upgrade") {
		t.Errorf("error %q does not include upgrade hint", err)
	}
}

func TestValidate_ForwardCompat_ExtraFields(t *testing.T) {
	// Future schema version may add fields. Older binaries must tolerate
	// unknown fields rather than refusing to validate the license.
	key := registerTestKey(t)
	now := time.Now()
	expires := now.Add(24 * time.Hour)
	issued := now.Add(-1 * time.Hour)
	payload := []byte(fmt.Sprintf(`{
		"license_id":   "lic_future",
		"customer_id":  "cust_future",
		"key_id":       %q,
		"expires_at":   %q,
		"max_nodes":    5,
		"issued_at":    %q,
		"future_field": "hello",
		"another":      {"nested": true}
	}`, key.keyID, expires.Format(time.RFC3339Nano), issued.Format(time.RFC3339Nano)))
	sig := ed25519.Sign(key.priv, payload)
	licenseKey := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(sig)

	got, status, err := Validate(licenseKey)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if status != LicenseStatusValid {
		t.Errorf("status = %v, want %v", status, LicenseStatusValid)
	}
	if got.LicenseID != "lic_future" {
		t.Errorf("LicenseID = %q, want %q", got.LicenseID, "lic_future")
	}
}

func TestValidate_WrongSignatureSize(t *testing.T) {
	key := registerTestKey(t)
	lic := newTestLicense(time.Now().Add(24 * time.Hour))
	licenseKey := signLicense(t, key, lic)
	parts := strings.Split(licenseKey, ".")

	// Truncate signature.
	tampered := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte("too short"))

	_, status, err := Validate(tampered)
	if err == nil || status != LicenseStatusInvalid {
		t.Errorf("expected invalid+error, got status=%v err=%v", status, err)
	}
	if !strings.Contains(err.Error(), "wrong size") {
		t.Errorf("error %q does not mention size", err)
	}
}

func TestDaysUntilExpiry(t *testing.T) {
	cases := []struct {
		name   string
		offset time.Duration
		want   int
	}{
		{"30 days ahead", 30 * 24 * time.Hour, 29}, // truncated by hours/24
		{"1 day ahead", 24 * time.Hour, 0},         // less than 24h once we hit the func
		{"already expired", -1 * time.Hour, -1},
		{"long expired", -100 * 24 * time.Hour, -101}, // truncation
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lic := &License{ExpiresAt: time.Now().Add(tc.offset)}
			got := lic.DaysUntilExpiry()
			// Allow ±1 wobble for clock movement during the test.
			if got < tc.want-1 || got > tc.want+1 {
				t.Errorf("DaysUntilExpiry = %d, want ~%d (±1)", got, tc.want)
			}
		})
	}
}

func TestLicenseStatus_String(t *testing.T) {
	cases := map[LicenseStatus]string{
		LicenseStatusInvalid:     "invalid",
		LicenseStatusValid:       "valid",
		LicenseStatusGracePeriod: "grace_period",
		LicenseStatusExpired:     "expired",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(s), got, want)
		}
	}
	// Unknown value falls through to the default branch.
	if got := LicenseStatus(99).String(); got != "LicenseStatus(99)" {
		t.Errorf("unknown status string = %q", got)
	}
}

func TestResolve(t *testing.T) {
	if _, ok := Resolve("key_prod_2026_04"); !ok {
		t.Error("Resolve(key_prod_2026_04) failed — production key missing from ring")
	}
	if _, ok := Resolve("does_not_exist"); ok {
		t.Error("Resolve(does_not_exist) returned ok=true")
	}
}
