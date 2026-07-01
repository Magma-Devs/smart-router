// Package probing is the home for the Smart Router's decision-plane probing
// logic: deciding which endpoints to actively probe, scoring their health, and
// driving recovery — as distinct from the data-plane block telemetry owned by
// package endpointstate (per-endpoint ChainTrackers and the relay-harvested
// latest-block state).
//
// The two planes are kept separate on purpose: the data plane (endpointstate)
// observes block tips and feeds telemetry; the decision plane (probing) consumes
// that telemetry to choose and score endpoints. Dependencies point one way —
// probing may read endpointstate; endpointstate must not depend on probing — so
// there are no back-arrows between observation and decision.
//
// This package is an intentionally empty shell created during the
// ChainTracker & Probing Redesign prep (MAG-2171) to stake out the boundary
// before the probing logic is extracted here from lavasession in a later ticket.
package probing
