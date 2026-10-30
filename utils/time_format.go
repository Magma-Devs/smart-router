package utils

import "time"

// FormatUTCRFC3339 renders t as UTC RFC3339, and the zero time as zero. A zero time.Time means
// "never happened", which is a different state from a real instant, so it must not render as the
// Go zero-date ("0001-01-01T00:00:00Z") that reads like 1 January year 1. Callers pick the word:
// the /debug/* state endpoints use "" so a test can check it cheaply; a log line uses "never".
func FormatUTCRFC3339(t time.Time, zero string) string {
	if t.IsZero() {
		return zero
	}
	return t.UTC().Format(time.RFC3339)
}
