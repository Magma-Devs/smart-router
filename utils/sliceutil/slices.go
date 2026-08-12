package sliceutil

import (
	"cmp"
	"slices"
)

type Number interface {
	~float32 | ~float64 | ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

type ComparableByFunc interface {
	Differentiator() string
}

func Slice[T any](v ...T) []T {
	return v
}

func Min[T cmp.Ordered](s []T) (m T) {
	if len(s) > 0 {
		m = s[0]
		for _, v := range s[1:] {
			if m > v {
				m = v
			}
		}
	}
	return m
}

func Max[T cmp.Ordered](s []T) (m T) {
	if len(s) > 0 {
		m = s[0]
		for _, v := range s[1:] {
			if m < v {
				m = v
			}
		}
	}
	return m
}

// Percentile returns the value at the given rank in the slice.
// If reverse is true, the slice is sorted in descending order.
// If reverse is false, the slice is sorted in ascending order.
func Percentile[T Number](slice []T, rank float64, reverse bool) T {
	data_len := len(slice)
	if data_len == 0 || rank < 0.0 || rank > 1.0 {
		return 0
	}
	if reverse {
		slices.SortFunc(slice, func(i, j T) int { return cmp.Compare(j, i) })
	} else {
		slices.Sort(slice)
	}

	// Calculate the position based on the rank
	position := int(float64(data_len-1) * rank)

	// Calculate the fractional part

	if data_len%2 == 0 {
		// Interpolate between two middle values
		lower := slice[position]
		upper := slice[position+1]
		return lower + T(float64(upper-lower)*rank)
	} else {
		return slice[position]
	}
}

// the bigger it is the more unstable the values in slice from the given argument "compare"
func Stability[T Number](slice []T, compare T) float64 {
	stabilitySum := 0.0
	for _, x := range slice {
		diff := x - compare
		if x < compare {
			diff = compare - x
		}
		stabilitySum += float64(diff) / float64(compare)
	}
	return stabilitySum / float64(len(slice))
}

func Contains[T comparable](slice []T, elem T) bool {
	for _, e := range slice {
		if e == elem {
			return true
		}
	}
	return false
}

func ContainsPredicate[T comparable](slice []T, predicate func(elem T) bool) bool {
	for _, e := range slice {
		if predicate(e) {
			return true
		}
	}
	return false
}

func Map[T, V any](slice []T, filter func(T) V) []V {
	values := make([]V, len(slice))
	for i := range slice {
		values[i] = filter(slice[i])
	}
	return values
}
