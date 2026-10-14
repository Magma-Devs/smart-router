package metrics

const (
	// CustomerOther is the overflow bucket for the customer label: a value past the
	// per-spec cap, or one too long to be an id. The value comes from the caller's
	// project-id header, so without a bound one client sending a fresh value per
	// request would mint a new series per request.
	CustomerOther = "other"

	// maxCustomersPerSpec caps how many distinct customer values one spec may
	// register. A tenant labelling its own customers sends a stable set (GK8: 100+,
	// MAG-4081), far under the cap; it binds only on a flood.
	maxCustomersPerSpec = 512

	// maxCustomerLabelLength bounds one value. An id is short; anything longer is
	// collapsed rather than stored, well below the label-value limit Mimir enforces.
	maxCustomerLabelLength = 128
)

func newCustomerRegistry() *batchSignatureRegistry {
	return &batchSignatureRegistry{cap: maxCustomersPerSpec, admitted: make(map[string]map[string]struct{})}
}

// normalizeCustomerLabel bounds the customer label: the first maxCustomersPerSpec
// distinct values per spec pass through, and everything after — or anything longer
// than maxCustomerLabelLength — collapses to CustomerOther.
//
// Admission is monotone (see batchSignatureRegistry), so a customer keeps its series
// for the life of the process. CustomerOther passes through unconditionally, which
// makes the collapse idempotent.
func (m *SmartRouterMetricsManager) normalizeCustomerLabel(spec, customer string) string {
	if customer == CustomerOther {
		return customer
	}
	if len(customer) > maxCustomerLabelLength || !m.customers.admit(spec, customer) {
		m.recordCustomerOverflow(spec)
		return CustomerOther
	}
	return customer
}

func (m *SmartRouterMetricsManager) recordCustomerOverflow(spec string) {
	if m == nil || m.customerOverflow == nil {
		return
	}
	m.customerOverflow.WithLabelValues(spec).Inc()
}
