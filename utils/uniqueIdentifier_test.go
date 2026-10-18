package utils

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A log reader turns a GUID into a time window by shifting it, so the top 32 bits must be the
// second it was minted.
func TestGenerateUniqueIdentifier_DecodesToItsSecond(t *testing.T) {
	before := time.Now().Unix()
	guid := GenerateUniqueIdentifier()
	after := time.Now().Unix()

	minted := int64(guid >> 32)
	require.GreaterOrEqual(t, minted, before)
	require.LessOrEqual(t, minted, after)

	// Still a decimal uint64 on the wire, the shape every log line and header already carries.
	parsed, err := strconv.ParseUint(strconv.FormatUint(guid, 10), 10, 64)
	require.NoError(t, err)
	require.Equal(t, guid, parsed)
}

// The dashboard and the relay_end event key a request by its GUID, and a router mints ten
// thousand in one second, so every GUID one process mints must be distinct.
func TestGenerateUniqueIdentifier_DistinctWithinAProcess(t *testing.T) {
	const draws = 100_000
	seen := make(map[uint64]struct{}, draws)
	for range draws {
		guid := GenerateUniqueIdentifier()
		if _, dup := seen[guid]; dup {
			t.Fatalf("guid %d minted twice", guid)
		}
		seen[guid] = struct{}{}
	}
}

// Every relay goroutine mints on its own, so the counter must stay distinct under concurrency.
func TestGenerateUniqueIdentifier_DistinctAcrossGoroutines(t *testing.T) {
	const workers, perWorker = 8, 20_000
	results := make([][]uint64, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := make([]uint64, perWorker)
			for i := range out {
				out[i] = GenerateUniqueIdentifier()
			}
			results[w] = out
		}()
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, workers*perWorker)
	for _, out := range results {
		for _, guid := range out {
			if _, dup := seen[guid]; dup {
				t.Fatalf("guid %d minted twice", guid)
			}
			seen[guid] = struct{}{}
		}
	}
}
