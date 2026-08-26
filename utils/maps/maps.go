package maps

import (
	"maps"
)

func ValuesSlice[T comparable, V any](in map[T]V) []V {
	return slicesFromIter(maps.Values(in))
}

// slicesFromIter collects an iter.Seq into a slice.
func slicesFromIter[T any](seq func(yield func(T) bool)) []T {
	var s []T
	for v := range seq {
		s = append(s, v)
	}
	return s
}
