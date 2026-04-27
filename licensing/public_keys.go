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
	"key_prod_2026_04": {
		0x9f, 0x1e, 0x39, 0xb7, 0xd2, 0xe8, 0x70, 0xc7,
		0x32, 0x78, 0xbe, 0x8f, 0x38, 0xdb, 0x0a, 0xe1,
		0x4c, 0x03, 0xb0, 0x6a, 0x53, 0xdf, 0xfc, 0x24,
		0x13, 0x97, 0x7c, 0x3d, 0xbe, 0xdb, 0xf8, 0x60,
	},
}

// Resolve looks up a signing key by its key_id. Returns nil, false if not found.
func Resolve(keyID string) (ed25519.PublicKey, bool) {
	k, ok := PublicKeys[keyID]
	return k, ok
}
