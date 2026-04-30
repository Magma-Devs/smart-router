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

const (
	// licenseFileEnvVar lets operators point at a license file outside the
	// working directory without shell scripting around the binary invocation.
	licenseFileEnvVar = "SMART_ROUTER_LICENSE_FILE"

	// defaultLicenseFilePath is read when neither --license-file nor
	// SMART_ROUTER_LICENSE_FILE is set. Co-located with the binary keeps
	// the simplest single-tenant deployment one file away from working.
	defaultLicenseFilePath = "./license.key"

	// licenseFileFlagName is the name of the highest-precedence override flag.
	licenseFileFlagName = "license-file"
)

// installLicenseCheck wires the enterprise license-validation PreRunE onto the
// router command and registers the --license-file flag. Validation runs lazily
// (only when the user actually invokes the router, not on `version` / `cache` /
// `test`). Dispatches on LicenseStatus per §3.3.5 of the implementation plan:
//
//   - Valid:        log INFO banner, ActivateConfig, start ExpiryWatcher.
//   - GracePeriod:  log ERROR, ActivateConfig (still operational), start watcher.
//   - Expired:      FATAL — past grace, refuse to start.
//   - Invalid/err:  FATAL — bad signature, unknown key, malformed envelope.
//   - File missing: FATAL — Sprint 6 chose fail-fast over silent community-mode
//     fallback. An enterprise binary without a license must not start.
//
// Wraps any existing PreRunE so a future contributor adding one inside
// rpcsmartrouter is preserved.
//
// Sprint 6 runtime file-loaded model — license envelope is read at startup
// from one of three sources (highest precedence first):
//  1. --license-file=PATH flag
//  2. $SMART_ROUTER_LICENSE_FILE env var
//  3. ./license.key default
//
// See docs/ENTERPRISE_LICENSING.md for the operator-facing guide.
func installLicenseCheck(cmd *cobra.Command) {
	cmd.Flags().String(licenseFileFlagName, "",
		"Path to enterprise license file. Highest precedence; overrides $"+licenseFileEnvVar+" and the default "+defaultLicenseFilePath+".")

	existing := cmd.PreRunE
	cmd.PreRunE = func(c *cobra.Command, args []string) error {
		envelope, source, err := loadLicenseEnvelope(c)
		if err != nil {
			utils.LavaFormatFatal("license file unreadable — enterprise binary cannot start without a valid license", err,
				utils.Attribute{Key: "source", Value: source})
			return nil // unreachable — LavaFormatFatal calls os.Exit(1)
		}
		utils.LavaFormatInfo("Loading enterprise license",
			utils.Attribute{Key: "source", Value: source})

		if err := validateAndActivateLicense(c.Context(), envelope); err != nil {
			return err
		}
		if existing != nil {
			return existing(c, args)
		}
		return nil
	}
}

// loadLicenseEnvelope resolves the license envelope from the three-tier
// precedence chain (flag > env > default) and returns a human-readable source
// description for operator logging. Pure relative to filesystem state; no
// side effects beyond os.ReadFile.
func loadLicenseEnvelope(cmd *cobra.Command) (envelope, source string, err error) {
	if flagPath, _ := cmd.Flags().GetString(licenseFileFlagName); flagPath != "" {
		envelope, err = licensing.LoadFromFile(flagPath)
		return envelope, fmt.Sprintf("--%s=%s", licenseFileFlagName, flagPath), err
	}
	envelope, path, fromEnv, err := licensing.LoadFromEnvOrFile(licenseFileEnvVar, defaultLicenseFilePath)
	if fromEnv {
		return envelope, fmt.Sprintf("$%s=%s", licenseFileEnvVar, path), err
	}
	return envelope, fmt.Sprintf("default %s", path), err
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
