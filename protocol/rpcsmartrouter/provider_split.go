package rpcsmartrouter

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/magma-Devs/smart-router/protocol/common"
	"github.com/magma-Devs/smart-router/protocol/lavasession"
	"github.com/magma-Devs/smart-router/utils"
)

// splitProvidersPerURL turns every configured provider into one single-url provider entity per
// expanded url, named "<provider>/<index>" (MAG-4219).
//
// The router keys every mechanism by provider name: the optimizer score, the blocked list, the
// retry skip-list, the pin header, the rate-limit hold-off, admission and re-verification, every
// rpc_endpoint_* series, every log line. A provider with several urls therefore shared one of
// each, and a lagging url hid behind a healthy sibling while the first usable url in config order
// served every relay. With one url per entity each of those is per url, and none of them changes.
//
// Per configured provider: entries that name the same node the same way (same url, internal path,
// credentials and declared add-ons) are folded into one; each url is expanded over the spec's
// internal paths exactly as the session builder used to do it; and every resulting url becomes an
// entity carrying the parent's chain, api-interface, stake and group label. The index
// is the url's position in the configured node-urls, fixed before merging and expansion, and an
// auto-expanded path url appends its path: chainstack/0, chainstack/1, chainstack/0/v2. A
// provider with one url is name/0 — always, so adding a second url later does not rename the
// first one's series.
//
// The config file is untouched; Parent on every entity keeps the configured name for the pin
// header. Output order is config order, so the list feeds session selection and metrics
// registration the same way on every boot.
func splitProvidersPerURL(
	providers []*lavasession.RPCStaticProviderEndpoint,
	internalPaths []string,
	servesSubscriptions func(internalPath string, declaredAddons []string) bool,
) []*lavasession.RPCStaticProviderEndpoint {
	entities := make([]*lavasession.RPCStaticProviderEndpoint, 0, len(providers))
	for _, provider := range providers {
		if provider == nil {
			continue
		}
		for _, node := range mergeSameNodeURLs(provider) {
			// Expanded per configured url, not per provider, so the index survives expansion
			// without a field on common.NodeUrl.
			for _, url := range expandInternalPaths([]common.NodeUrl{node.url}, internalPaths, servesSubscriptions) {
				entity := *provider
				entity.Name = entityName(provider.Name, node.index, node.url, url)
				entity.Parent = provider.Name
				entity.NodeUrls = []common.NodeUrl{url}
				entities = append(entities, &entity)
			}
		}
	}
	return entities
}

// entityName is "<parent>/<index>" for a configured url and "<parent>/<index><path>" for a url
// expandInternalPaths generated from it, so the versions a shared root serves are told apart:
// chainstack/0 is the root, chainstack/0/v2 and chainstack/0/v3 its paths. A url that declares
// its own internal-path is a configured url and gets a plain index.
func entityName(parent string, index int, configured, expanded common.NodeUrl) string {
	name := parent + "/" + strconv.Itoa(index)
	if expanded.InternalPath != configured.InternalPath {
		name += expanded.InternalPath
	}
	return name
}

type indexedNodeURL struct {
	index int
	url   common.NodeUrl
}

// mergeSameNodeURLs folds configured entries that declare the same node the same way into one,
// keeping config order and the first entry's index. Same means the same url, internal path,
// credentials AND add-ons. One url listed under two api keys is two nodes as far as the operator
// can tell, and stays two entities. One url listed with two different add-on sets ([archive] and
// [debug]) also stays two entities, on purpose: the spec verifier builds a per-url route table from
// the declared add-ons, and a url declaring an extension together with an add-on ([archive, debug])
// is a shape it refuses — it strips every add-on at once for the extension-less copy, so "debug
// without archive" has no route. Two entries with one add-on each are routable; their union is not.
// Keeping them apart is what main did with two urls, and each gets its own name and score here.
// What this folds is the same url listed twice with the same add-ons (the chart's double archive
// entry), which used to be dropped on chains with internal paths and to share one tracker
// everywhere else (MAG-4025).
func mergeSameNodeURLs(provider *lavasession.RPCStaticProviderEndpoint) []indexedNodeURL {
	merged := make([]indexedNodeURL, 0, len(provider.NodeUrls))
	position := make(map[string]int, len(provider.NodeUrls))
	for i, url := range provider.NodeUrls {
		key := nodeKey(url)
		at, seen := position[key]
		if !seen {
			position[key] = len(merged)
			merged = append(merged, indexedNodeURL{index: i, url: url})
			continue
		}
		merged[at].url = mergeNodeURL(provider.Name, merged[at].index, merged[at].url, url)
	}
	return merged
}

// nodeKey identifies a node declaration: its url, the spec path it serves, the add-ons it declares
// (as a set) and the credentials it is reached with. Two entries with equal keys are one node
// declared twice.
func nodeKey(url common.NodeUrl) string {
	auth := url.AuthConfig
	headers := make([]string, 0, len(auth.AuthHeaders))
	for name := range auth.AuthHeaders {
		headers = append(headers, name)
	}
	sort.Strings(headers)
	addons := append([]string(nil), url.Addons...)
	sort.Strings(addons)
	var b strings.Builder
	b.WriteString(url.Url)
	b.WriteByte(0)
	b.WriteString(url.InternalPath)
	b.WriteByte(0)
	b.WriteString(strings.Join(addons, ","))
	for _, name := range headers {
		b.WriteByte(0)
		b.WriteString(name)
		b.WriteByte('=')
		b.WriteString(auth.AuthHeaders[name])
	}
	fmt.Fprintf(&b, "\x00%s\x00%t\x00%t\x00%s\x00%s\x00%s",
		auth.AuthQuery, auth.UseTLS, auth.AllowInsecure, auth.KeyPem, auth.CertPem, auth.CaCert)
	return b.String()
}

// mergeNodeURL folds `other` into `kept`, the first entry for this node. The add-ons are equal by
// construction (part of the node key); the union only drops a duplicate. The other lists that
// declare what the node serves are unioned; standalone-addons holds only if every entry opted out
// of the base collection, since an entry that did not opt out claims the base collection for the
// node. The connection settings are the first entry's; a difference is an operator mistake worth a
// warning, not a reason to refuse a config that runs in production today.
func mergeNodeURL(parent string, index int, kept, other common.NodeUrl) common.NodeUrl {
	kept.Addons = unionStrings(kept.Addons, other.Addons)
	kept.SkipVerifications = unionStrings(kept.SkipVerifications, other.SkipVerifications)
	kept.Methods = unionStrings(kept.Methods, other.Methods)
	kept.StandaloneAddons = kept.StandaloneAddons && other.StandaloneAddons

	var differ []string
	if kept.Timeout != other.Timeout {
		differ = append(differ, "timeout")
	}
	if kept.IpForwarding != other.IpForwarding {
		differ = append(differ, "ip-forwarding")
	}
	if kept.AcceptEncoding != other.AcceptEncoding {
		differ = append(differ, "accept-encoding")
	}
	if !reflect.DeepEqual(kept.GrpcConfig, other.GrpcConfig) {
		differ = append(differ, "grpc-config")
	}
	if len(differ) > 0 {
		utils.LavaFormatWarning("the same node is listed twice under one provider with different settings; keeping the first entry's", nil,
			utils.LogAttr("provider", parent),
			utils.LogAttr("index", index),
			utils.LogAttr("fields", differ),
		)
	}
	utils.LavaFormatInfo("merged a node listed twice under one provider into one entity",
		utils.LogAttr("provider", parent),
		utils.LogAttr("index", index),
		utils.LogAttr("addons", kept.Addons),
	)
	return kept
}

// unionStrings appends to a the members of b it does not already hold, in order. a is returned
// as it is when b adds nothing, so an entry listed once keeps its list untouched.
func unionStrings(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, s := range list {
			if _, dup := seen[s]; dup {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
