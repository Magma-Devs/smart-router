package sliceutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	mapFunc := func(_ int) int { return 10 }
	require.Equal(t, []int{}, Map([]int{}, mapFunc))
	require.Equal(t, []int{10}, Map([]int{1}, mapFunc))
	require.Equal(t, []int{10, 10, 10}, Map([]int{1, 2, 3}, mapFunc))
}
