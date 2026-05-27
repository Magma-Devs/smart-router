package licensing

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// GracePeriod is how long after a license's ExpiresAt it still validates as
// in-grace. Past this window, Validate returns LicenseStatusExpired and the
// startup path treats it as fatal.
const GracePeriod = 14 * 24 * time.Hour

// License is the signed payload embedded in every license key.
//
// All-or-nothing model: a valid license unlocks every enterprise capability.
// No per-feature flags, no tier field. If tiering is ever needed, add a
// schema-version bump and a Features field — older binaries reject licenses
// with unknown future fields cleanly via the "unknown signing key" upgrade
// path (since rotated keys signal the new schema).
type License struct {
	LicenseID  string    `json:"license_id"`
	CustomerID string    `json:"customer_id"`
	KeyID      string    `json:"key_id"`
	ExpiresAt  time.Time `json:"expires_at"`
	MaxNodes   int       `json:"max_nodes"` // 0 = unlimited. Honor-system only: signed into the payload, not enforced anywhere at runtime.
	IssuedAt   time.Time `json:"issued_at"`
}

// LicenseStatus is the result of Validate. Callers dispatch on this value
// rather than a bool so grace-period behavior is expressible at the call site.
type LicenseStatus int

const (
	// LicenseStatusInvalid means malformed, bad signature, unknown key id, or
	// any other parse failure. Always paired with a non-nil error.
	LicenseStatusInvalid LicenseStatus = iota
	// LicenseStatusValid means in force — accept and proceed normally.
	LicenseStatusValid
	// LicenseStatusGracePeriod means expired but within GracePeriod —
	// startup succeeds with an ERROR log warning.
	LicenseStatusGracePeriod
	// LicenseStatusExpired means past the grace window — startup must reject.
	LicenseStatusExpired
)

func (s LicenseStatus) String() string {
	switch s {
	case LicenseStatusInvalid:
		return "invalid"
	case LicenseStatusValid:
		return "valid"
	case LicenseStatusGracePeriod:
		return "grace_period"
	case LicenseStatusExpired:
		return "expired"
	default:
		return fmt.Sprintf("LicenseStatus(%d)", int(s))
	}
}

// Validate parses and verifies a license key. Returns (license, status, error).
// A non-nil error always implies LicenseStatusInvalid. Callers should dispatch
// on the returned status; the License pointer is non-nil for any non-Invalid
// status so callers can read LicenseID, ExpiresAt, etc. for logging.
//
// License key envelope: base64url(json_payload) + "." + base64url(signature).
// The signature is computed over the raw JSON payload bytes (not the base64
// encoding), using the Ed25519 private key whose key_id is in the payload.
//
// Order of checks matters:
//  1. Structural parsing (split, base64, json).
//  2. Key lookup by KeyID — only the KeyID is trusted at this point.
//  3. Signature verification — nothing else in the payload is trusted yet.
//  4. Expiry vs current time, dispatching to grace / expired status.
//
// We never trust ExpiresAt before signature verification.
func Validate(licenseKey string) (*License, LicenseStatus, error) {
	parts := strings.Split(licenseKey, ".")
	if len(parts) != 2 {
		return nil, LicenseStatusInvalid, errors.New("license format invalid: expected payload.signature")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, LicenseStatusInvalid, fmt.Errorf("license payload not valid base64url: %w", err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, LicenseStatusInvalid, fmt.Errorf("license signature not valid base64url: %w", err)
	}
	if len(sig) != ed25519.SignatureSize {
		return nil, LicenseStatusInvalid, fmt.Errorf("license signature wrong size: got %d, want %d", len(sig), ed25519.SignatureSize)
	}
	var lic License
	if err := json.Unmarshal(payload, &lic); err != nil {
		return nil, LicenseStatusInvalid, fmt.Errorf("license payload not valid JSON: %w", err)
	}
	pub, ok := Resolve(lic.KeyID)
	if !ok {
		return nil, LicenseStatusInvalid, fmt.Errorf("unknown signing key %q — upgrade smart-router to a newer release", lic.KeyID)
	}
	if !ed25519.Verify(pub, payload, sig) {
		return nil, LicenseStatusInvalid, errors.New("license signature verification failed")
	}
	now := time.Now()
	switch {
	case now.Before(lic.ExpiresAt):
		return &lic, LicenseStatusValid, nil
	case now.Before(lic.ExpiresAt.Add(GracePeriod)):
		return &lic, LicenseStatusGracePeriod, nil
	default:
		return &lic, LicenseStatusExpired, nil
	}
}

// DaysUntilExpiry returns the number of whole days before ExpiresAt.
// Negative if already expired. Used by the startup banner and the
// expiry-warning goroutine in cmd/smartrouter.
func (l *License) DaysUntilExpiry() int {
	return int(time.Until(l.ExpiresAt).Hours() / 24)
}
