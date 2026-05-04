// Package licensing implements license verification for the Smart Router
// enterprise edition.
//
// The package is built into both community and enterprise binaries. Community
// builds never call Validate (no license is required). Enterprise builds call
// Validate at startup and dispatch on the returned LicenseStatus.
package licensing

import "crypto/ed25519"

// PublicKeys is the Ed25519 verification key ring keyed by key_id.
//
// Production keys are committed here. New keys are added when the prior key is
// rotated (alongside the new one — both verify until the old one is retired).
// Compromised keys are removed in a hot-fix release.
//
// Tests register ephemeral keys in this map and clean up via t.Cleanup. The
// committed entries below are production-only.
var PublicKeys = map[string]ed25519.PublicKey{
	"key_prod_2026_04": ed25519.PublicKey{
		0xb1, 0xbb, 0x6a, 0x1f, 0x00, 0x5b, 0x4f, 0x15,
		0xcc, 0x57, 0xc7, 0xee, 0x23, 0x32, 0x0c, 0xa8,
		0x0d, 0x8f, 0x85, 0x6a, 0xbb, 0xe1, 0xfe, 0xa5,
		0xf6, 0xb8, 0xdd, 0x58, 0x50, 0xdf, 0x28, 0x50,
	},
}

// Resolve looks up a signing key by its key_id. Returns nil, false if not found.
func Resolve(keyID string) (ed25519.PublicKey, bool) {
	k, ok := PublicKeys[keyID]
	return k, ok
}
