package sliceutil

func Map[T, V any](slice []T, filter func(T) V) []V {
	values := make([]V, len(slice))
	for i := range slice {
		values[i] = filter(slice[i])
	}
	return values
}
