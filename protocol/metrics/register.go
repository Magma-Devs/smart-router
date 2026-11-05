package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterFailurePolicy is what RegisterOrReuse does when registering fails for a reason other
// than the same collector already being registered.
type RegisterFailurePolicy func(err error)

// PanicOnRegisterFailure is the policy for collectors the process must not run without.
func PanicOnRegisterFailure(err error) { panic(err) }

// KeepUnregisteredOnFailure keeps the unregistered collector without a word: it still counts
// in-process, and /metrics lacks the series.
func KeepUnregisteredOnFailure(error) {}

// RegisterOrReuse registers c with registerer and returns the collector Prometheus actually
// exposes: c, or the identical collector registered before it (tests and repeated init register
// twice). Any other failure goes to onFailure, and c is returned unregistered if it returns.
func RegisterOrReuse[T prometheus.Collector](registerer prometheus.Registerer, c T, onFailure RegisterFailurePolicy) T {
	if err := registerer.Register(c); err != nil {
		are := prometheus.AlreadyRegisteredError{}
		if errors.As(err, &are) {
			if existing, ok := are.ExistingCollector.(T); ok {
				return existing
			}
		}
		onFailure(err)
	}
	return c
}
