package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MAG-4154: a negative --epoch-duration is refused at startup. The epoch timer divides by
// it, so the current epoch wraps to a huge number and the next boundary lands in the past.
// Zero stays allowed: it means unset, and the run path substitutes StandaloneEpochDuration.
func TestValidateEpochDuration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   time.Duration
		wantErr bool
	}{
		{name: "unset", value: 0},
		{name: "the default", value: StandaloneEpochDuration},
		{name: "one hour", value: time.Hour},
		{name: "minus one nanosecond", value: -time.Nanosecond, wantErr: true},
		{name: "minus five minutes", value: -5 * time.Minute, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEpochDuration(tc.value)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), EpochDurationFlag)
				return
			}
			require.NoError(t, err)
		})
	}
}
