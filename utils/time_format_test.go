package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatUTCRFC3339(t *testing.T) {
	require.Equal(t, "", FormatUTCRFC3339(time.Time{}, ""))
	require.Equal(t, "never", FormatUTCRFC3339(time.Time{}, "never"))
	at := time.Date(2026, 8, 27, 22, 58, 2, 0, time.FixedZone("IDT", 3*3600))
	require.Equal(t, "2026-08-27T19:58:02Z", FormatUTCRFC3339(at, "never"))
}
