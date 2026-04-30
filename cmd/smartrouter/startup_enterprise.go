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

// validateAndActivateLicense parses licenseKey, validates its signature/expiry,
// and promotes activeConfig to enterpriseConfig if valid (or in grace). Fatal
// paths exit the process; non-fatal paths return nil so the router can proceed.
//
// licenseKey is passed as a parameter (rather than read from
// licensing.EmbeddedLicense() internally) so tests can mint arbitrary
// license states with ephemeral keys.
func validateAndActivateLicense(ctx context.Context, licenseKey string) error {
	license, status, err := licensing.Validate(licenseKey)
	switch {
	case err != nil:
		// Steps 1–6 of Validate (envelope/signature/unknown-key) — exit hard.
		// LavaFormatFatal calls os.Exit(1) after logging.
		utils.LavaFormatFatal("license validation failed", err)
	case status == licensing.LicenseStatusInvalid:
		// Defensive — Validate should have returned err alongside Invalid; this
		// arm exists so a future Validate refactor that decouples the two
		// signals doesn't silently allow an Invalid license to start the router.
		utils.LavaFormatFatal("license invalid", nil)
	case status == licensing.LicenseStatusExpired:
		gracePeriodEnded := license.ExpiresAt.Add(licensing.GracePeriod).Format("2006-01-02")
		expiredOn := license.ExpiresAt.Format("2006-01-02")
		msg := fmt.Sprintf("license expired on %s (grace period ended %s) — re-issue and rebuild", expiredOn, gracePeriodEnded)
		utils.LavaFormatFatal(msg, nil,
			utils.Attribute{Key: "license_id", Value: license.LicenseID},
			utils.Attribute{Key: "customer_id", Value: license.CustomerID})
	case status == licensing.LicenseStatusGracePeriod:
		utils.LavaFormatError("LICENSE IN GRACE PERIOD — re-issue and rebuild before grace ends", nil,
			utils.Attribute{Key: "license_id", Value: license.LicenseID},
			utils.Attribute{Key: "customer_id", Value: license.CustomerID},
			utils.Attribute{Key: "expired_on", Value: license.ExpiresAt.Format("2006-01-02")},
			utils.Attribute{Key: "days_until_expiry", Value: license.DaysUntilExpiry()})
		rpcsmartrouter.ActivateConfig(license)
		go expiryWatcher(ctx, license)
	case status == licensing.LicenseStatusValid:
		utils.LavaFormatInfo("Smart Router ENTERPRISE Edition",
			utils.Attribute{Key: "license_id", Value: license.LicenseID},
			utils.Attribute{Key: "customer_id", Value: license.CustomerID},
			utils.Attribute{Key: "expires_at", Value: license.ExpiresAt.Format("2006-01-02")},
			utils.Attribute{Key: "days_until_expiry", Value: license.DaysUntilExpiry()})
		rpcsmartrouter.ActivateConfig(license)
		go expiryWatcher(ctx, license)
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
		utils.Attribute{Key: "customer_id", Value: license.CustomerID},
		utils.Attribute{Key: "expires_at", Value: license.ExpiresAt.Format("2006-01-02")},
		utils.Attribute{Key: "days_until_expiry", Value: days})
}