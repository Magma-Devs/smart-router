package chainlib

import (
	"context"
	"errors"
	"testing"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	spectypes "github.com/magma-Devs/smart-router/types/spec"
	"github.com/stretchr/testify/require"
)

// MAG-3326. Admission used to be all-or-nothing: Validate returned on the first
// Fail-severity verification, so a provider serving the base collection perfectly
// and failing ONE service's verification was excluded from everything.
//
// The container shapes below are the ones specs/ actually produce. An earlier
// draft of this feature keyed everything on Addon and its tests manufactured
// {Addon:"archive"} — a container no spec ever builds — so the suite was green
// while the motivating case was unfixed.

// archiveOnBaseCollection is how every EVM and cosmos spec keys the archive check:
// on the BASE collection (add_on ""), scoped by extension, severity omitted (Fail).
// Verified against specs/ethereum.json.
func archiveOnBaseCollection() VerificationContainer {
	return VerificationContainer{
		Name:            "pruning",
		Value:           "0x0",
		Severity:        spectypes.ParseValue_Fail,
		VerificationKey: VerificationKey{Addon: "", Extension: "archive"},
		ParseDirective:  spectypes.ParseDirective{FunctionTag: spectypes.FUNCTION_TAG_VERIFICATION, ApiName: "pruning"},
	}
}

// archiveScopedOnTraceCollection is ethereum's trace collection carrying an
// archive-scoped pruning value: {Addon:"trace", Extension:"archive"}.
func archiveScopedOnTraceCollection() VerificationContainer {
	v := archiveOnBaseCollection()
	v.VerificationKey = VerificationKey{Addon: "trace", Extension: "archive"}
	return v
}

func addonScoped(name, addon string) VerificationContainer {
	v := archiveOnBaseCollection()
	v.Name = name
	v.VerificationKey = VerificationKey{Addon: addon}
	return v
}

func baseCollectionOnly(name string) VerificationContainer {
	v := archiveOnBaseCollection()
	v.Name = name
	v.VerificationKey = VerificationKey{}
	return v
}

func TestRefusedServiceFor(t *testing.T) {
	// Extension first: it is the narrower thing, and it is what was probed.
	require.Equal(t, "archive", refusedServiceFor(archiveOnBaseCollection()),
		"the motivating case — charging this to Addon finds \"\" and goes fatal")
	require.Equal(t, "archive", refusedServiceFor(archiveScopedOnTraceCollection()),
		"the node's archive-ness failed, not its trace support")
	require.Equal(t, "debug", refusedServiceFor(addonScoped("enabled", "debug")))
	require.Equal(t, "", refusedServiceFor(baseCollectionOnly("chain-id")),
		"nothing narrower to fall back to — stays fatal")
}

func TestAdmittedServices(t *testing.T) {
	t.Run("refuses only the named service", func(t *testing.T) {
		url := common.NodeUrl{Url: "https://a", Addons: []string{"archive", "debug"}}
		var admission ProviderAdmission
		admission.fail(url, "archive")

		services, keep := admission.AdmittedServices(url)
		require.True(t, keep)
		require.Equal(t, []string{"debug"}, services)
		require.Equal(t, []string{"archive", "debug"}, url.Addons, "the config is never mutated")
	})

	t.Run("keyed by url and internal path", func(t *testing.T) {
		root := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
		v2 := common.NodeUrl{Url: "https://a", InternalPath: "/v2", Addons: []string{"archive"}}
		var admission ProviderAdmission
		admission.fail(v2, "archive")

		services, _ := admission.AdmittedServices(root)
		require.Equal(t, []string{"archive"}, services, "the root path keeps it")
		services, _ = admission.AdmittedServices(v2)
		require.Empty(t, services)
	})

	// The regression an earlier draft shipped: emptying a standalone url's service
	// list flips ServesBaseCollection() to true, handing a node its operator said
	// cannot answer the base collection straight back to base traffic.
	t.Run("a standalone url that kept nothing is dropped", func(t *testing.T) {
		url := common.NodeUrl{Url: "https://evm", Addons: []string{"evm"}, StandaloneAddons: true}
		require.False(t, url.ServesBaseCollection())

		var admission ProviderAdmission
		admission.fail(url, "evm")
		services, keep := admission.AdmittedServices(url)
		require.False(t, keep, "nothing left to serve — must not become a base-collection endpoint")
		require.Empty(t, services)
	})

	t.Run("an ordinary url that kept nothing is still served", func(t *testing.T) {
		url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
		var admission ProviderAdmission
		admission.fail(url, "archive")
		services, keep := admission.AdmittedServices(url)
		require.True(t, keep, "it still serves the base collection")
		require.Empty(t, services)
	})

	t.Run("nothing refused returns the input without copying", func(t *testing.T) {
		url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
		var admission ProviderAdmission
		services, keep := admission.AdmittedServices(url)
		require.True(t, keep)
		// Identity, not deep equality: require.Equal compares contents and would
		// pass even if this allocated a fresh slice on every session rebuild.
		require.True(t, unsafe.SliceData(services) == unsafe.SliceData(url.Addons))
	})
}

func newValidationFetcher(t *testing.T, url common.NodeUrl, verifications []VerificationContainer, verifyErr error) *ChainFetcher {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	parser := NewMockChainParser(ctrl)
	parser.EXPECT().GetVerifications(gomock.Any(), gomock.Any(), gomock.Any()).Return(verifications, nil).AnyTimes()
	router := NewMockChainRouter(ctrl)
	if verifyErr != nil {
		// Verify returns at CraftChainMessage, so SendNodeMsg is never reached on
		// the failure path — no expectation for it, or ctrl.Finish would not notice.
		parser.EXPECT().CraftMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, verifyErr).AnyTimes()
	} else {
		parser.EXPECT().CraftMessage(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("unused")).AnyTimes()
	}

	return &ChainFetcher{
		endpoint: &lavasession.RPCProviderEndpoint{
			ChainID: "ETH1", ApiInterface: spectypes.APIInterfaceJsonRPC,
			NodeUrls: []common.NodeUrl{url},
		},
		chainParser: parser,
		chainRouter: router,
	}
}

// TestValidateCollections_ArchiveUrlKeepsTheProvider is the ticket's motivating
// case, in the shape specs/ethereum.json produces: "attach an archive url to an
// otherwise healthy provider, have it fail the archive check, lose the provider".
func TestValidateCollections_ArchiveUrlKeepsTheProvider(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
	verifications := []VerificationContainer{archiveOnBaseCollection()}

	t.Run("per-collection refuses only archive", func(t *testing.T) {
		fetcher := newValidationFetcher(t, url, verifications, errors.New("not an archive node"))
		admission, err := fetcher.ValidateCollections(context.Background())
		require.NoError(t, err, "the provider survives a failure that belongs to one service")
		require.True(t, admission.Any())

		services, keep := admission.AdmittedServices(url)
		require.True(t, keep)
		require.Empty(t, services, "archive is refused; the base collection is untouched")
	})

	t.Run("the all-or-nothing path is unchanged", func(t *testing.T) {
		fetcher := newValidationFetcher(t, url, verifications, errors.New("not an archive node"))
		require.Error(t, fetcher.Validate(context.Background()))
	})
}

// TestValidateCollections_ExtensionScopedFailureSparesTheAddon: what failed is the
// node's archive-ness, so refusing "trace" would drop working traffic and leave the
// failing extension advertised.
func TestValidateCollections_ExtensionScopedFailureSparesTheAddon(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"trace", "archive"}, StandaloneAddons: true}
	fetcher := newValidationFetcher(t, url, []VerificationContainer{archiveScopedOnTraceCollection()}, errors.New("not an archive node"))

	admission, err := fetcher.ValidateCollections(context.Background())
	require.NoError(t, err)
	services, keep := admission.AdmittedServices(url)
	require.True(t, keep)
	require.Equal(t, []string{"trace"}, services, "trace works and is kept; archive failed and is refused")
}

// TestValidateCollections_BaseFailureIsStillFatal: a failure with no service to
// attribute it to excludes the provider, and does NOT quietly turn the url into a
// standalone-addons one.
func TestValidateCollections_BaseFailureIsStillFatal(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
	fetcher := newValidationFetcher(t, url,
		[]VerificationContainer{baseCollectionOnly("chain-id"), archiveOnBaseCollection()}, errors.New("down"))

	_, err := fetcher.ValidateCollections(context.Background())
	require.Error(t, err, "a node that cannot serve the base collection has nothing narrower to fall back to")
}

func TestValidateCollections_SkippedVerificationsRefuseNothing(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
	url.SkipVerifications = []string{common.SkipVerificationsWildcard}

	fetcher := newValidationFetcher(t, url, []VerificationContainer{archiveOnBaseCollection()}, errors.New("down"))
	admission, err := fetcher.ValidateCollections(context.Background())
	require.NoError(t, err)
	require.False(t, admission.Any(), "a verification that never ran cannot refuse anything")
}

// TestValidateCollections_HealthyProviderRefusesNothing covers the path every
// healthy provider takes at boot, which no earlier test exercised — every fixture
// failed, so a build where admission.fail were unreachable would have passed.
func TestValidateCollections_HealthyProviderRefusesNothing(t *testing.T) {
	url := common.NodeUrl{Url: "https://a", Addons: []string{"archive"}}
	// A verification with neither a value nor a latest distance is inactive, so
	// Verify returns nil without a relay — a genuine pass, not a skip.
	inactive := archiveOnBaseCollection()
	inactive.Value = ""
	inactive.LatestDistance = 0

	fetcher := newValidationFetcher(t, url, []VerificationContainer{inactive}, nil)
	admission, err := fetcher.ValidateCollections(context.Background())
	require.NoError(t, err)
	require.False(t, admission.Any())

	services, keep := admission.AdmittedServices(url)
	require.True(t, keep)
	require.Equal(t, []string{"archive"}, services)
}
