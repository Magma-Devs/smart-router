package chainlib

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/magma-Devs/smart-router/protocol/common"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	specutils "github.com/magma-Devs/smart-router/utils/keeper"
	"github.com/stretchr/testify/require"
)

// What a failed check costs (MAG-3950). A value whose spec sets no severity takes the
// default for where its check sits; an explicit severity says what it costs.
func TestEffectiveSeverity(t *testing.T) {
	basic := baseCollectionOnly("chain-id")
	archive := archiveOnBaseCollection()
	debug := addonScoped("enabled", "debug")
	with := func(v VerificationContainer, severity spectypes.ParseValue_VerificationSeverity) VerificationContainer {
		v.Severity = severity
		return v
	}
	for _, tc := range []struct {
		name         string
		verification VerificationContainer
		want         spectypes.ParseValue_VerificationSeverity
	}{
		{"default basic check: the url goes", with(basic, spectypes.ParseValue_Default), spectypes.ParseValue_Fail},
		{"default archive check: archive goes", with(archive, spectypes.ParseValue_Default), spectypes.ParseValue_PartialFail},
		{"default add-on check: the add-on goes", with(debug, spectypes.ParseValue_Default), spectypes.ParseValue_PartialFail},
		{"Fail on an add-on check: the add-on is mandatory", with(debug, spectypes.ParseValue_Fail), spectypes.ParseValue_Fail},
		{"PartialFail on a basic check: nothing smaller to switch off", with(basic, spectypes.ParseValue_PartialFail), spectypes.ParseValue_Fail},
		{"PartialFail on an add-on check", with(debug, spectypes.ParseValue_PartialFail), spectypes.ParseValue_PartialFail},
		{"Warning anywhere costs nothing", with(archive, spectypes.ParseValue_Warning), spectypes.ParseValue_Warning},
	} {
		require.Equal(t, tc.want, EffectiveSeverity(tc.verification), tc.name)
	}
}

// An explicit Fail on an add-on check makes the add-on mandatory: its failure takes
// the url out, where the default refuses only that add-on. A Warning refuses nothing.
func TestValidateCollections_TheSeverityDecidesWhatAFailureCosts(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
	admit := func(severity spectypes.ParseValue_VerificationSeverity) (ProviderAdmission, error) {
		archive := archiveOnBaseCollection()
		archive.Severity = severity
		fetcher := newValidationFetcher(t, url, []VerificationContainer{archive}, errors.New("not an archive node"))
		return fetcher.ValidateCollections(context.Background())
	}

	admission, err := admit(spectypes.ParseValue_Default)
	require.NoError(t, err, "the default refuses only archive")
	services, keep := admission.AdmittedServices(url)
	require.True(t, keep)
	require.Empty(t, services)

	_, err = admit(spectypes.ParseValue_Fail)
	require.ErrorContains(t, err, "no node url left serving the root path", "a mandatory archive takes its url, the provider's only one")

	admission, err = admit(spectypes.ParseValue_Warning)
	require.NoError(t, err)
	require.False(t, admission.Any(), "a Warning refuses nothing")
}

// Every value the bundled specs carry costs exactly what it did before severities: the
// router used to treat every failure but a Warning as Fail, charging it to the url for
// a basic check and to its one service for an add-on or extension check. A value whose
// new severity would cost something else fails this, so a spec change that means it
// has to say so here (MAG-3950). SEVERITY_GUARD_SPECS points it at another spec
// directory, lava-specs' for one, with a floor of SEVERITY_GUARD_MIN values.
func TestEffectiveSeverity_EveryBundledSpecValueCostsWhatItDidBefore(t *testing.T) {
	dir, floor := "../../specs/", 100
	if other := os.Getenv("SEVERITY_GUARD_SPECS"); other != "" {
		dir, floor = other, 0
		if n, err := strconv.Atoi(os.Getenv("SEVERITY_GUARD_MIN")); err == nil {
			floor = n
		}
	}
	indexes := bundledSpecIndexes(t, dir)
	require.NotEmpty(t, indexes)
	checked := 0
	for _, index := range indexes {
		spec, err := specutils.GetSpecFromLocalDirs([]string{dir}, index)
		require.NoError(t, err, index)
		for _, collection := range spec.ApiCollections {
			for _, verification := range collection.Verifications {
				for _, value := range verification.Values {
					container := VerificationContainer{
						Name:            verification.Name,
						Severity:        value.Severity,
						VerificationKey: VerificationKey{Addon: collection.CollectionData.AddOn, Extension: value.Extension},
					}
					before := spectypes.ParseValue_Warning
					if value.Severity != spectypes.ParseValue_Warning {
						before = spectypes.ParseValue_PartialFail
						if refusedServiceFor(container) == "" {
							before = spectypes.ParseValue_Fail
						}
					}
					require.Equal(t, before, EffectiveSeverity(container), "%s %s %s/%s", index, verification.Name, collection.CollectionData.AddOn, value.Extension)
					checked++
				}
			}
		}
	}
	require.Greater(t, checked, floor, "the specs carry this many values at least, or this test reads none")
	t.Logf("%d values in %d specs cost what they did before", checked, len(indexes))
}

func bundledSpecIndexes(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	require.NoError(t, err)
	var indexes []string
	for _, file := range files {
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		var doc struct {
			Proposal struct {
				Specs []struct {
					Index string `json:"index"`
				} `json:"specs"`
			} `json:"proposal"`
		}
		if json.Unmarshal(raw, &doc) != nil {
			continue
		}
		for _, spec := range doc.Proposal.Specs {
			if strings.TrimSpace(spec.Index) != "" {
				indexes = append(indexes, spec.Index)
			}
		}
	}
	return indexes
}

// A failing Warning check is kept apart from refusals: it refuses nothing and never
// moves a session, and a pass that a rate limit or the deadline kept from answering
// it keeps what the last pass knew, as a throttled service does (MAG-3950).
func TestAdmissionFrom_AWarningIsKeptApartFromRefusals(t *testing.T) {
	url := common.NodeUrl{Url: "https://a"}
	warning := baseCollectionOnly("tokens-owner-indexed")
	warning.Severity = spectypes.ParseValue_Warning
	admit := func(err error) ProviderAdmission {
		admission, serving := admissionFrom([]nodeUrlOutcome{{url: url, served: url, checks: []checkOutcome{{verification: warning, err: err}}}})
		require.NoError(t, serving)
		return admission
	}

	failed := admit(errors.New("no owner index"))
	require.Equal(t, []Warning{{Check: "tokens-owner-indexed"}}, failed.Warnings())
	require.False(t, failed.Any())
	require.True(t, failed.Equal(ProviderAdmission{}))
	require.Nil(t, failed.Throttled())
	require.Equal(t, failed.Warnings(), failed.WithoutURLRefusals(nil).Warnings(), "the url grace keeps it")

	for name, err := range map[string]error{"rate-limited": throttledErr(), "not run": errCheckNotRun} {
		undecided := admit(err)
		require.Empty(t, undecided.Warnings(), "%s is not a failure", name)
		require.NotNil(t, undecided.Throttled(), "%s makes re-verification keep the last pass", name)
		require.Equal(t, failed.Warnings(), undecided.KeepingThrottledFrom(failed).Warnings(), name)
		require.Empty(t, undecided.KeepingThrottledFrom(ProviderAdmission{}).Warnings(), "%s does not invent one", name)
	}
	require.Empty(t, ProviderAdmission{}.KeepingThrottledFrom(failed).Warnings(), "a check that passed clears it")
}
