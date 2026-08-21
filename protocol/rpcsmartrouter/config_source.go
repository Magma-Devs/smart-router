package rpcsmartrouter

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/magma-Devs/smart-router/utils"
	"github.com/spf13/viper"
)

// Config resolution for the commands that take one — `smartrouter [config]` and
// `smartrouter health [config]` — lives here so the two cannot drift apart.
//
// Viper locates a config in one of two mutually exclusive ways, and the argument used to
// be handed to the wrong one for path-shaped values. SetConfigName treats its value as a
// *name* joined onto every search path, and filepath.Join swallows the leading separator
// of an absolute path — Join(".", "/etc/smartrouter/config.yml") is
// "etc/smartrouter/config.yml". An absolute path was therefore looked for *inside* each
// search directory, could never exist there, and failed against directories the operator
// never named (MAG-2861). Relative paths worked only by accident, because "." happens to
// be a search path.
//
// So a path-shaped argument now goes to SetConfigFile, while a bare name keeps the
// search-path lookup it has always had. The branch decides how a failure is *explained* —
// against the one path the operator named, or against the directories a name is searched
// in — not where the file is found: a relative path is still resolved against every search
// path, because that is what SetConfigName did for it and what operators rely on.

// configSearchPaths are the directories a config argument is resolved against, in
// precedence order — both a bare *name* and a *relative* path. An absolute path names its
// file outright and is not resolved against them.
var configSearchPaths = []string{".", "./config", defaultNodeHome}

// defaultConfigType is the format assumed for a config whose extension does not declare
// one — including the extension-less names the search-path lookup accepts. Every
// smartrouter config is YAML.
const defaultConfigType = "yml"

// pointViperAtConfig aims the global viper at the config selected by the command's
// arguments. Exactly one argument names the config; anything else (none, or inline
// endpoint triplets) selects the default config name.
//
// It reports the target it settled on, for use in messages, and whether that target is an
// exact file rather than a name searched for across configSearchPaths — which decides how
// a failure to read it should be explained.
func pointViperAtConfig(args []string) (target string, isFile bool) {
	target = DefaultRPCSmartRouterFileName
	if len(args) == 1 {
		target = args[0]
	}

	if isConfigFilePath(target) {
		target = resolveConfigFilePath(target)
		viper.SetConfigFile(target)
		// Let a recognized extension declare the format, and fall back to YAML for
		// anything else. Set it either way: without an explicit type an extension-less
		// path fails as UnsupportedConfigError instead of being read at all.
		if ext := strings.TrimPrefix(filepath.Ext(target), "."); slices.Contains(viper.SupportedExts, ext) {
			viper.SetConfigType(ext)
		} else {
			viper.SetConfigType(defaultConfigType)
		}
		return target, true
	}

	viper.SetConfigName(target)
	viper.SetConfigType(defaultConfigType)
	for _, path := range configSearchPaths {
		viper.AddConfigPath(path)
	}
	return target, false
}

// isConfigFilePath reports whether the argument names a config file directly, rather than
// a name to look up across the search paths.
//
// The test is lexical — does the value name a directory component? — and deliberately does
// not consult the filesystem. What is being decided here is what the operator *meant*, and
// that cannot depend on which files happen to sit in the working directory: an earlier cut
// of this fix keyed on os.Stat, which made a bare `smartrouter router` load an
// extension-less ./router in preference to the ./router.yml the search path has always
// returned. A name with no separator is a name, always, and goes to the lookup that has
// always handled it.
func isConfigFilePath(arg string) bool {
	if arg == "" {
		return false
	}
	return filepath.IsAbs(arg) || strings.ContainsRune(arg, '/') || strings.ContainsRune(arg, filepath.Separator)
}

// resolveConfigFilePath returns the file an explicit path refers to.
//
// A *relative* path is probed against configSearchPaths, in the same precedence order a
// bare name is, because that is what SetConfigName + AddConfigPath already did for it.
// filepath.Join only swallows a *leading* separator, so Join("./config",
// "smartrouter_examples/eth.yml") resolved perfectly well — only absolute paths were
// demoted (MAG-2861). Dropping the search paths for relative paths would have narrowed
// resolution rather than fixed it: `smartrouter smartrouter_examples/smartrouter_eth.yml`
// from the project root has always loaded config/smartrouter_examples/smartrouter_eth.yml.
//
// An absolute path is never joined onto a search path. That join is the demotion this
// ticket removed, and a search path cannot contribute anything to an already-rooted path.
//
// Within each candidate a path carrying no recognized config extension is tried with each
// supported extension appended, in viper's own order and before the bare path, exactly as
// searchInPath does — which is why `smartrouter config/akash` loads config/akash.yml and
// why an extension-less ./router must not shadow ./router.yml.
//
// When nothing resolves, the path as given is returned, so viper fails against the file
// the operator actually named and the error can say so.
func resolveConfigFilePath(arg string) string {
	for _, candidate := range configFilePathCandidates(arg) {
		if resolved, found := resolveWithSupportedExt(candidate); found {
			return resolved
		}
	}
	return arg
}

// configFilePathCandidates lists the paths an explicit path argument may refer to, in
// precedence order. A relative path is joined onto each search path — the first of which is
// ".", so the working directory is tried first and unchanged. An absolute path refers only
// to itself.
func configFilePathCandidates(arg string) []string {
	if filepath.IsAbs(arg) {
		return []string{arg}
	}
	candidates := make([]string, 0, len(configSearchPaths))
	for _, searchPath := range configSearchPaths {
		// Viper runs a search path through absPathify, which expands the $HOME that the
		// defaultNodeHome literal carries. filepath.Join would keep it verbatim, so the
		// expansion has to happen here for $HOME/.smartrouter to mean the same thing in both
		// branches.
		candidates = append(candidates, filepath.Join(os.ExpandEnv(searchPath), arg))
	}
	return candidates
}

// resolveWithSupportedExt reports the file a single candidate resolves to, mirroring
// searchInPath: every supported extension is appended and tried first, in viper's own
// order, and only then the candidate as given.
//
// The appended forms are tried even when the candidate already ends in a supported
// extension, because searchInPath appends to v.configName unconditionally — it never checks
// whether the name already carries one. `sub/router.yml` has therefore always been able to
// resolve to sub/router.yml.json, and short-circuiting on a recognized extension (which
// looks obviously right) is the one remaining way the two branches could disagree. Pathological
// as that layout is, "strictly additive" has to mean it.
func resolveWithSupportedExt(candidate string) (resolved string, found bool) {
	for _, ext := range viper.SupportedExts {
		if withExt := candidate + "." + ext; isExistingFile(withExt) {
			return withExt, true
		}
	}
	return candidate, isExistingFile(candidate)
}

// isExistingFile reports whether path is a regular file that can be stat-ed. A directory is
// not a config.
func isExistingFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// isConfigNotFound reports whether err means "no config file was there", which viper
// phrases three different ways: ConfigFileNotFoundError when it searched and came up empty,
// a plain fs.ErrNotExist when it was pointed at an exact file, and EISDIR when that exact
// path turned out to be a directory. All three are operator mistakes worth a clean message;
// anything else (malformed YAML, a permissions problem) is not, and must stay on the loud
// path.
//
// The directory case only arises for an explicit path — a search of configSearchPaths
// skips directories and reports ConfigFileNotFoundError — and without it `smartrouter
// ./config` would take the FormatFatal stack-dump path this fix exists to remove.
func isConfigNotFound(err error) bool {
	var notFound viper.ConfigFileNotFoundError
	return errors.As(err, &notFound) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.EISDIR)
}

// configNotFoundMessage explains where the config was looked for, in the terms the
// operator used — and, importantly, describes the search that actually happened. An
// absolute path is resolved against nothing but itself, so naming search paths for it is
// the MAG-2861 confusion; a relative path *is* probed across configSearchPaths, so claiming
// otherwise is the same confusion pointed the other way.
func configNotFoundMessage(target string, isFile bool) string {
	if isFile {
		if info, err := os.Stat(target); err == nil && info.IsDir() {
			return "the given config path is a directory, not a config file: " + target
		}
		if !filepath.IsAbs(target) {
			return "config file not found: " + target +
				" — a relative path is resolved against each of the search paths"
		}
		return "config file not found at the given path: " + target
	}
	return "no config file found — pass a config file as an argument (e.g. `smartrouter <config-file>.yml`, " +
		"absolute or relative), or place " + DefaultRPCSmartRouterFileName + " in one of the search paths"
}

// configLocationAttributes describe where the config was looked for, for attachment to a
// not-found error. A relative target is reported alongside the working directory it was
// resolved against and the full list of paths actually probed, since neither is visible
// from the log line — and reporting only the working directory would describe a narrower
// search than the one that ran.
func configLocationAttributes(target string, isFile bool) []utils.Attribute {
	cwd, err := os.Getwd()
	if err != nil {
		cwd = "<unknown>"
	}
	if isFile {
		resolved := target
		if abs, err := filepath.Abs(target); err == nil {
			resolved = abs
		}
		attributes := []utils.Attribute{
			{Key: "config_file", Value: target},
			{Key: "resolved_path", Value: resolved},
			{Key: "working_directory", Value: cwd},
		}
		if !filepath.IsAbs(target) {
			// The exact candidate list, not a restatement of configSearchPaths: it is what
			// was probed, and it saves the operator from having to perform the join in
			// their head to see where the file was expected.
			attributes = append(attributes, utils.Attribute{
				Key: "searched_paths", Value: strings.Join(configFilePathCandidates(target), " "),
			})
		}
		return attributes
	}
	return []utils.Attribute{
		{Key: "config_name", Value: target},
		{Key: "search_paths", Value: strings.Join(configSearchPaths, " ")},
		{Key: "working_directory", Value: cwd},
	}
}
