// Pull-request gates: the unit-coverage check and the e2e-test check. A module
// of its own, like tools/wizard, so CI tooling stays out of the router module.
// Standard library only.
module github.com/magma-Devs/smart-router/tools/prgate

go 1.26.6
