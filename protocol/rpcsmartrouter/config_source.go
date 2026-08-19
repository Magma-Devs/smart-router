package rpcsmartrouter

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

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
// So a path-shaped argument now goes to SetConfigFile and is loaded verbatim, while a
// bare name keeps the search-path lookup it has always had.

// configSearchPaths are the directories a bare config *name* is resolved against, in
// precedence order.
var configSearchPaths = []string{".", "./config", lavaDefaultNodeHome}

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
// A path carrying no recognized config extension is also tried with each supported
// extension appended, in viper's own order, because that is what the search-path lookup
// does inside a directory: `smartrouter config/akash` has always loaded config/akash.yml,
// and a lexical split that skipped this would have broken it. Extensions are tried before
// the bare path for the same reason — searchInPath does — so resolution is identical
// whichever branch an argument takes.
//
// When nothing resolves, the path as given is returned, so viper fails against the file
// the operator actually named and the error can say so.
func resolveConfigFilePath(arg string) string {
	if ext := strings.TrimPrefix(filepath.Ext(arg), "."); slices.Contains(viper.SupportedExts, ext) {
		return arg // already declares its format; nothing to append
	}
	for _, ext := range viper.SupportedExts {
		if candidate := arg + "." + ext; isExistingFile(candidate) {
			return candidate
		}
	}
	return arg
}

// isExistingFile reports whether path is a regular file that can be stat-ed. A directory is
// not a config.
func isExistingFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// isConfigNotFound reports whether err means "no config file was there", which viper
// phrases two different ways: ConfigFileNotFoundError when it searched and came up empty,
// and a plain fs.ErrNotExist when it was pointed at an exact file. Both are operator
// mistakes worth a clean message; anything else (malformed YAML, a permissions problem) is
// not.
func isConfigNotFound(err error) bool {
	var notFound viper.ConfigFileNotFoundError
	return errors.As(err, &notFound) || errors.Is(err, fs.ErrNotExist)
}

// configNotFoundMessage explains where the config was looked for, in the terms the
// operator used: the one path they named, or the search paths their bare name was
// resolved against.
func configNotFoundMessage(target string, isFile bool) string {
	if isFile {
		return "config file not found at the given path: " + target
	}
	return "no config file found — pass a config file as an argument (e.g. `smartrouter <config-file>.yml`, " +
		"absolute or relative), or place " + DefaultRPCSmartRouterFileName + " in one of the search paths"
}

// configLocationAttributes describe where the config was looked for, for attachment to a
// not-found error. A relative target is reported alongside the working directory it was
// resolved against, since that is the piece an operator cannot see from the log line.
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
		return []utils.Attribute{
			{Key: "config_file", Value: target},
			{Key: "resolved_path", Value: resolved},
			{Key: "working_directory", Value: cwd},
		}
	}
	return []utils.Attribute{
		{Key: "config_name", Value: target},
		{Key: "search_paths", Value: strings.Join(configSearchPaths, " ")},
		{Key: "working_directory", Value: cwd},
	}
}
