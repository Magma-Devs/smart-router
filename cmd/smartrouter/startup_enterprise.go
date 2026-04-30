//go:build enterprise

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/Magma-Devs/smart-router/licensing"
	"github.com/Magma-Devs/smart-router/protocol/rpcsmartrouter"
	"github.com/Magma-Devs/smart-router/utils"
)

// installLicenseCheck wires the enterprise license-validation PreRunE onto the
// router command. Validation runs lazily (only when the user actually invokes
// the router, not on `version` / `cache` / `test`). Dispatches on LicenseStatus
// per §3.3.5 of the implementation plan:
//
//   - Valid:        log INFO banner, ActivateConfig, start ExpiryWatcher.
//   - GracePeriod:  log ERROR, ActivateConfig (still operational), start watcher.
//   - Expired:      FATAL — past grace, refuse to start.
//   - Invalid/err:  FATAL — bad signature, unknown key, malformed envelope.
//
// Wraps any existing PreRunE so a future contributor adding one inside
// rpcsmartrouter is preserved.
func installLicenseCheck(cmd *cobra.Command) {
	existing := cmd.PreRunE
	cmd.PreRunE = func(c *cobra.Command, args []string) error {
		if err := validateAndActivateLicense(c.Context(), licensing.EmbeddedLicense()); err != nil {
			return err
		}
		if existing != nil {
			return existing(c, args)
		}
		return nil
	}
}

// licenseDecision is the pure-logic output of resolveLicense — what the
// command layer should do, without yet doing it. Splitting "compute the
// decision" from "apply side effects" makes every branch (including FATAL
// paths) unit-testable without a subprocess test.
//
// Field semantics:
//   - fatalMsg non-empty → caller MUST call utils.LavaFormatFatal(fatalMsg, err, attrs...).
//   - license non-nil → the validated *licensing.License (Valid, Grace, or Expired).
//   - status         → the licensing.LicenseStatus (Invalid/Valid/Grace/Expired).
//   - err            → the underlying validation error if any (nil for Grace/Valid).
type licenseDecision struct {
	fatalMsg string
	license  *licensing.License
	status   licensing.LicenseStatus
	err      error
}

// shouldFatal reports whether the caller should exit fatally with the
// captured fatalMsg. Convenience wrapper for tests that don't want to
// reach into the struct directly.
func (d licenseDecision) shouldFatal() bool { return d.fatalMsg != "" }

// resolveLicense parses + validates licenseKey and returns the decision the
// command layer should act on. Pure function — no logging, no os.Exit, no
// goroutines spawned. Tests assert on the returned struct.
func resolveLicense(licenseKey string) licenseDecision {
	license, status, err := licensing.Validate(licenseKey)
	switch {
	case err != nil:
		// Validate's steps 1–6 (envelope/signature/unknown-key/malformed).
		return licenseDecision{fatalMsg: "license validation failed", err: err, status: status}
	case status == licensing.LicenseStatusInvalid:
		// Defensive — Validate should have returned err alongside Invalid; this
		// arm exists so a future Validate refactor that decouples the two
		// signals doesn't silently allow an Invalid license to start the router.
		return licenseDecision{fatalMsg: "license invalid", status: status}
	case status == licensing.LicenseStatusExpired:
		expiredOn := license.ExpiresAt.Format("2006-01-02")
		gracePeriodEnded := license.ExpiresAt.Add(licensing.GracePeriod).Format("2006-01-02")
		return licenseDecision{
			fatalMsg: fmt.Sprintf("license expired on %s (grace period ended %s) — re-issue and rebuild", expiredOn, gracePeriodEnded),
			license:  license,
			status:   status,
		}
	case status == licensing.LicenseStatusGracePeriod, status == licensing.LicenseStatusValid:
		return licenseDecision{license: license, status: status}
	default:
		return licenseDecision{fatalMsg: fmt.Sprintf("unknown license status %v", status), status: status}
	}
}

// validateAndActivateLicense interprets the resolveLicense decision, applies
// side effects (logging + ActivateConfig + expiryWatcher launch), and returns
// nil for non-fatal paths. Fatal paths call utils.LavaFormatFatal which
// invokes os.Exit(1) and never returns.
//
// Side effects are concentrated here; logic-under-test lives in resolveLicense.
func validateAndActivateLicense(ctx context.Context, licenseKey string) error {
	d := resolveLicense(licenseKey)

	if d.shouldFatal() {
		attrs := []utils.Attribute{}
		if d.license != nil {
			attrs = append(attrs,
				utils.Attribute{Key: "license_id", Value: d.license.LicenseID},
				utils.Attribute{Key: "customer", Value: d.license.CustomerID})
		}
		utils.LavaFormatFatal(d.fatalMsg, d.err, attrs...)
		return nil // unreachable — LavaFormatFatal calls os.Exit(1)
	}

	switch d.status {
	case licensing.LicenseStatusGracePeriod:
		expiredOn := d.license.ExpiresAt.Format("2006-01-02")
		gracePeriodEnded := d.license.ExpiresAt.Add(licensing.GracePeriod).Format("2006-01-02")
		utils.LavaFormatError(
			fmt.Sprintf("LICENSE IN GRACE PERIOD — expired %s, stops accepting new starts on %s", expiredOn, gracePeriodEnded),
			nil,
			utils.Attribute{Key: "license_id", Value: d.license.LicenseID},
			utils.Attribute{Key: "customer", Value: d.license.CustomerID},
			utils.Attribute{Key: "expires", Value: expiredOn},
			utils.Attribute{Key: "days_until_expiry", Value: d.license.DaysUntilExpiry()})
		rpcsmartrouter.ActivateConfig(d.license)
		go expiryWatcher(ctx, d.license)
	case licensing.LicenseStatusValid:
		utils.LavaFormatInfo("Smart Router ENTERPRISE Edition",
			utils.Attribute{Key: "license_id", Value: d.license.LicenseID},
			utils.Attribute{Key: "customer", Value: d.license.CustomerID},
			utils.Attribute{Key: "expires", Value: d.license.ExpiresAt.Format("2006-01-02")},
			utils.Attribute{Key: "days_until_expiry", Value: d.license.DaysUntilExpiry()})
		rpcsmartrouter.ActivateConfig(d.license)
		go expiryWatcher(ctx, d.license)
	}
	return nil
}

// expiryWatcher logs license-expiry warnings on a cadence that scales with
// time-to-expiry: hourly during grace, daily for the last 30 days, hourly for
// the last 7 days. Outside the warning window (>30 days from expiry) it polls
// once a day so a long-running process eventually enters the warning window.
//
// Returns when ctx is cancelled. Tests pass t.Context() to avoid goroutine
// leaks across runs; production passes the Cobra command context (lives for
// the process lifetime).
func expiryWatcher(ctx context.Context, license *licensing.License) {
	for {
		days := license.DaysUntilExpiry()
		warnInterval := watcherCadence(days)
		if warnInterval > 0 {
			logExpiryWarning(license, days)
		}
		// If no warning was logged this iteration we still poll daily so a
		// long-running process detects entering the warning window.
		sleep := warnInterval
		if sleep == 0 {
			sleep = 24 * time.Hour
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

// watcherCadence returns the warning-log cadence based on days-until-expiry.
// Returns 0 when no warning is yet warranted (>30 days from expiry).
func watcherCadence(daysUntilExpiry int) time.Duration {
	switch {
	case daysUntilExpiry < 0:
		return time.Hour // grace period — already past expiry
	case daysUntilExpiry < 7:
		return time.Hour // last 7 days
	case daysUntilExpiry < 30:
		return 24 * time.Hour // last 30 days
	default:
		return 0
	}
}

func logExpiryWarning(license *licensing.License, days int) {
	msg := "license approaching expiry"
	if days < 0 {
		msg = "LICENSE IN GRACE PERIOD"
	}
	utils.LavaFormatError(msg, nil,
		utils.Attribute{Key: "license_id", Value: license.LicenseID},
		utils.Attribute{Key: "customer", Value: license.CustomerID},
		utils.Attribute{Key: "expires", Value: license.ExpiresAt.Format("2006-01-02")},
		utils.Attribute{Key: "days_until_expiry", Value: days})
}
