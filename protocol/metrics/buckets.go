package metrics

// latencyBuckets is the shared histogram bucket set for all latency metrics
// in milliseconds. Starting at 1ms ensures cache hits (0–5ms) and fast
// in-process operations are resolved into distinct buckets rather than being
// collapsed into a single catch-all ≤10ms bucket.
var latencyBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}

// cacheEntryBytesBuckets sizes cache entries from 1 KiB to 64 MiB in steps of 4, so the
// default 1 MiB entry cap (--cache-max-entry-bytes) falls on a bucket boundary.
var cacheEntryBytesBuckets = []float64{1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20, 1 << 22, 1 << 24, 1 << 26}
