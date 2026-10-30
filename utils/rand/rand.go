package rand

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	mathrand "math/rand"
	mathrandv2 "math/rand/v2"
	"sync/atomic"
)

func generateSeed(data []byte) int64 {
	sum256 := sha256.Sum256(data)
	seed := int64(binary.LittleEndian.Uint64(sum256[0:8]))
	return seed
}

// New returns a new deterministic PRNG instance seeded from the given data.
// This is used for consensus-critical operations where deterministic randomness is required.
func New(data []byte) *mathrand.Rand {
	seed := generateSeed(data)
	source := mathrand.NewSource(seed)
	return mathrand.New(source)
}

// Seed re-seeds an existing deterministic PRNG instance with new data.
func Seed(rng *mathrand.Rand, data []byte) {
	seed := generateSeed(data)
	rng.Seed(seed)
}

// cryptoSource is a math/rand/v2 Source that draws every value from crypto/rand. It holds no
// state, so the Rand built on it is safe for concurrent use without a lock. The global
// protocolRand uses it: security and uniformity are prioritized over determinism.
type cryptoSource struct{}

func (cryptoSource) Uint64() uint64 {
	var b [8]byte
	cryptorand.Read(b[:]) // never returns an error; crashes the program if the OS source fails
	return binary.LittleEndian.Uint64(b[:])
}

func newProtocolRand() *mathrandv2.Rand {
	return mathrandv2.New(cryptoSource{})
}

// protocolRand is an atomic pointer so InitRandomSeed/SetSpecificSeed (called from test setup, and
// once at process start) cannot race the Initialized()/PanicIf.../Intn... readers under -race.
var protocolRand atomic.Pointer[mathrandv2.Rand]

func Initialized() bool {
	return protocolRand.Load() != nil
}

func InitRandomSeed() {
	// Seed is no longer needed as crypto/rand is self-seeding
	// This function is kept for API compatibility
	protocolRand.Store(newProtocolRand())
}

func SetSpecificSeed(seed int64) {
	// Seed is no longer used as crypto/rand is self-seeding and non-deterministic
	// This function is kept for API compatibility but has no effect
	// For deterministic randomness, use New(data) instead
	_ = seed
	protocolRand.Store(newProtocolRand())
}

func PanicIfProtocolRandNotInitialized() {
	if protocolRand.Load() == nil {
		panic("rand.InitRandomSeed() must be called before using the rand package")
	}
}

func Intn(n int) int {
	PanicIfProtocolRandNotInitialized()
	if n <= 0 {
		panic("invalid argument to Intn")
	}
	return protocolRand.Load().IntN(n)
}

func Float64() float64 {
	PanicIfProtocolRandNotInitialized()
	return protocolRand.Load().Float64()
}

func Uint64() uint64 {
	PanicIfProtocolRandNotInitialized()
	return protocolRand.Load().Uint64()
}

func Int63() int64 {
	PanicIfProtocolRandNotInitialized()
	return protocolRand.Load().Int64()
}
