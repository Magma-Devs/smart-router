//go:build enterprise

package licensing

import "testing"

func TestEmbeddedLicense_NotEmpty(t *testing.T) {
	if EmbeddedLicense() == "" {
		t.Fatal("EmbeddedLicense() is empty — embedded_license.txt is missing or blank")
	}
}

func TestEmbeddedLicense_Validates(t *testing.T) {
	envelope := EmbeddedLicense()
	lic, status, err := Validate(envelope)
	if err != nil {
		t.Fatalf("Validate(embedded license): %v", err)
	}
	switch status {
	case LicenseStatusValid:
		t.Logf("embedded license valid; expires %s (%d days)", lic.ExpiresAt, lic.DaysUntilExpiry())
	case LicenseStatusGracePeriod:
		t.Errorf("embedded license is in grace period — re-issue and rebuild before release. expires=%s", lic.ExpiresAt)
	case LicenseStatusExpired:
		t.Errorf("embedded license has EXPIRED — re-issue and rebuild. expired=%s", lic.ExpiresAt)
	default:
		t.Errorf("unexpected status %v", status)
	}
}
