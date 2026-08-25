package rpcsmartrouter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// writeConfig drops a config carrying a distinctive marker, so a test can assert *which*
// of several same-named candidates viper actually loaded rather than merely that it loaded
// something.
func writeConfig(t *testing.T, path, marker string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("marker: "+marker+"\n"), 0o644))
	return path
}

// TestPointViperAtConfigResolution is the regression net for MAG-2861: an absolute config
// path used to be handed to viper as a config *name* and joined onto each search path
// (filepath.Join(".", "/abs/x.yml") == "abs/x.yml"), so it could never resolve. Each case
// asserts both that the config loads and that the *expected* file loaded.
func TestPointViperAtConfigResolution(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup lays out files under the temp working directory and returns the args to
		// pass, plus the marker of the file that must end up loaded.
		setup func(t *testing.T, dir string) (args []string, wantMarker string)
	}{
		{
			// The bug: this failed with "Not Found in [. ./config $HOME/.lava]".
			name: "absolute path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				abs := writeConfig(t, filepath.Join(dir, "elsewhere", "router.yml"), "absolute")
				return []string{abs}, "absolute"
			},
		},
		{
			// An absolute path is loaded verbatim, so nothing appends an extension —
			// the format has to be assumed. Every smartrouter config is YAML.
			name: "absolute path without extension",
			setup: func(t *testing.T, dir string) ([]string, string) {
				abs := writeConfig(t, filepath.Join(dir, "elsewhere", "router"), "extensionless")
				return []string{abs}, "extensionless"
			},
		},
		{
			// The form every scripts/pre_setups/ launcher uses, after cd-ing to the
			// project root. It worked before only because "." is a search path.
			name: "relative path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "smartrouter_examples", "eth.yml"), "relative")
				return []string{"config/smartrouter_examples/eth.yml"}, "relative"
			},
		},
		{
			name: "bare name resolved from the config search path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "myconfig.yml"), "searched")
				return []string{"myconfig"}, "searched"
			},
		},
		{
			// A name that carries an extension is still a name: the search path has to
			// find it, since it names no directory of its own.
			name: "bare name with extension resolved from the config search path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "myconfig.yml"), "searched-ext")
				return []string{"myconfig.yml"}, "searched-ext"
			},
		},
		{
			name: "no argument falls back to the default config name",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, DefaultRPCSmartRouterFileName), "default")
				return nil, "default"
			},
		},
		{
			// Inline "host:port chain api-interface" triplets are not a config name;
			// the default config is selected exactly as when no argument is given.
			name: "inline endpoint args fall back to the default config name",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, DefaultRPCSmartRouterFileName), "default-inline")
				return []string{"127.0.0.1:3333", "ETH1", "jsonrpc"}, "default-inline"
			},
		},
		{
			// The working directory wins over the ./config shadow, matching the search
			// path's own precedence order — resolution must not change with the branch
			// taken.
			name: "working directory wins over the config directory",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "myconfig.yml"), "cwd")
				writeConfig(t, filepath.Join(dir, "config", "myconfig.yml"), "shadowed")
				return []string{"myconfig.yml"}, "cwd"
			},
		},
		{
			// An extension-less file must not shadow the .yml a bare name has always
			// resolved to. Keying the path/name split on os.Stat instead of on the shape
			// of the argument got this wrong: `router` found ./router and stopped.
			name: "bare name prefers the extension over an extensionless file",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "router"), "extensionless-shadow")
				writeConfig(t, filepath.Join(dir, "router.yml"), "yml-wins")
				return []string{"router"}, "yml-wins"
			},
		},
		{
			// A path carrying no extension still resolves, the way it always has via the
			// "." search path. This is the case that makes the lexical split safe.
			name: "relative path without extension resolves to the yml file",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "akash.yml"), "path-ext-appended")
				return []string{"config/akash"}, "path-ext-appended"
			},
		},
		{
			// The same precedence inside the explicit-path branch, so an argument resolves
			// identically no matter which branch its shape sends it down.
			name: "path prefers the extension over an extensionless file",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "router"), "extensionless-shadow")
				writeConfig(t, filepath.Join(dir, "config", "router.yml"), "yml-wins")
				return []string{"config/router"}, "yml-wins"
			},
		},
		{
			// An explicit "./" is intent, not decoration.
			name: "dot-slash relative path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "router.yml"), "dot-slash")
				return []string{"./router.yml"}, "dot-slash"
			},
		},
		{
			// The regression review caught: SetConfigName joined a *relative* path onto
			// every search path perfectly well — filepath.Join only swallows a leading
			// separator — so `smartrouter smartrouter_examples/smartrouter_eth.yml` from the
			// project root has always loaded it out of ./config. Sending path-shaped
			// arguments to SetConfigFile without re-probing the search paths narrowed
			// resolution instead of fixing it.
			name: "relative path resolves through the config search path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "smartrouter_examples", "eth.yml"), "via-config-dir")
				return []string{"smartrouter_examples/eth.yml"}, "via-config-dir"
			},
		},
		{
			// The same, for the last search path. lavaDefaultNodeHome is the literal
			// "$HOME/.lava"; viper expands it through absPathify, so the path branch has to
			// expand it too or this resolves against a directory named "$HOME".
			name: "relative path resolves through the lava home search path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				home := t.TempDir()
				t.Setenv("HOME", home)
				writeConfig(t, filepath.Join(home, ".lava", "sub", "router.yml"), "via-lava-home")
				return []string{"sub/router.yml"}, "via-lava-home"
			},
		},
		{
			// Precedence must not change with the branch: the working directory is the first
			// search path, so it wins over the ./config shadow for a path exactly as it does
			// for a name.
			name: "relative path prefers the working directory over the config directory",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "sub", "router.yml"), "cwd-wins")
				writeConfig(t, filepath.Join(dir, "config", "sub", "router.yml"), "shadowed")
				return []string{"sub/router.yml"}, "cwd-wins"
			},
		},
		{
			// Extension appending applies inside a search path too, since that is what
			// searchInPath did for the whole joined name.
			name: "extensionless relative path resolves through the config search path",
			setup: func(t *testing.T, dir string) ([]string, string) {
				writeConfig(t, filepath.Join(dir, "config", "smartrouter_examples", "eth.yml"), "via-config-dir-ext")
				return []string{"smartrouter_examples/eth"}, "via-config-dir-ext"
			},
		},
		{
			// MAG-2861 itself, stated as an invariant rather than a symptom: an absolute path
			// must never be joined onto a search path. Join("./config", "/abs/x.yml") is
			// "config/abs/x.yml" — the demotion this ticket removed — so a file planted at
			// exactly that shadow location must lose to the real one.
			name: "absolute path is not resolved against the search paths",
			setup: func(t *testing.T, dir string) ([]string, string) {
				abs := writeConfig(t, filepath.Join(dir, "elsewhere", "router.yml"), "absolute-wins")
				writeConfig(t, filepath.Join(dir, "config", abs), "demoted-shadow")
				return []string{abs}, "absolute-wins"
			},
		},
		{
			// searchInPath appends every supported extension to v.configName without ever
			// checking whether it already ends in one, so `sub/router.yml` has always been
			// able to resolve to sub/router.yml.json. A short-circuit on "already declares
			// its format" looks obviously correct and silently drops that — the last way the
			// two branches could disagree. Pathological layout, but "strictly additive"
			// has to mean it.
			name: "path with a supported extension still prefers an appended extension",
			setup: func(t *testing.T, dir string) ([]string, string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "router.yml.json"),
					[]byte(`{"marker":"appended-wins"}`), 0o644))
				writeConfig(t, filepath.Join(dir, "sub", "router.yml"), "bare-loses")
				return []string{"sub/router.yml"}, "appended-wins"
			},
		},
		{
			// The same rule in the name branch, which never stopped doing it — this is the
			// case that makes the one above an equivalence rather than a quirk.
			name: "name with a supported extension still prefers an appended extension",
			setup: func(t *testing.T, dir string) ([]string, string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "router.yml.json"),
					[]byte(`{"marker":"appended-wins-name"}`), 0o644))
				writeConfig(t, filepath.Join(dir, "router.yml"), "bare-loses")
				return []string{"router.yml"}, "appended-wins-name"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// t.TempDir can hand back a symlinked path on macOS (/var -> /private/var);
			// resolve it so an absolute-path assertion compares like for like.
			if resolved, err := filepath.EvalSymlinks(dir); err == nil {
				dir = resolved
			}
			t.Chdir(dir)
			viper.Reset()
			t.Cleanup(viper.Reset)

			args, wantMarker := tc.setup(t, dir)

			_, _ = pointViperAtConfig(args)
			require.NoError(t, viper.ReadInConfig())
			require.Equal(t, wantMarker, viper.GetString("marker"),
				"loaded %q, which is not the config this case names", viper.ConfigFileUsed())
		})
	}
}

// TestPointViperAtConfigNotFound covers the other half of MAG-2861: the message. Pointing
// viper at an exact file makes a miss an fs.ErrNotExist rather than a
// viper.ConfigFileNotFoundError, and the router's clean-error branch keyed only on the
// latter — so a typo'd path would have taken the LavaFormatFatal stack-dump path instead.
func TestPointViperAtConfigNotFound(t *testing.T) {
	t.Run("missing absolute path is a clean not-found naming that path", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		viper.Reset()
		t.Cleanup(viper.Reset)

		missing := filepath.Join(dir, "nope", "router.yml")
		target, isFile := pointViperAtConfig([]string{missing})
		require.True(t, isFile, "an absolute path must be loaded as a file, not searched for as a name")

		err := viper.ReadInConfig()
		require.Error(t, err)
		require.True(t, isConfigNotFound(err), "a missing explicit file must classify as not-found, got %v", err)

		msg := configNotFoundMessage(target, isFile)
		require.Contains(t, msg, missing, "the message must name the path the operator gave")
		require.NotContains(t, msg, "search paths",
			"an absolute path is resolved against nothing but itself — mentioning search paths for one is what sent MAG-2861 the wrong way")

		for _, attr := range configLocationAttributes(target, isFile) {
			require.NotEqual(t, "searched_paths", attr.Key,
				"an absolute path has exactly one candidate; listing it as a search would be noise")
		}
	})

	t.Run("missing relative path names the path and the roots it was searched under", func(t *testing.T) {
		// The point of MAG-2861 is that the diagnostics must describe the search that
		// actually ran. A relative path *is* probed across configSearchPaths, so the message
		// has to say so and the attributes have to list the candidates — an earlier cut of
		// this asserted the search paths "were never consulted", which stopped being true
		// the moment relative paths were resolved against them again.
		dir := t.TempDir()
		t.Chdir(dir)
		viper.Reset()
		t.Cleanup(viper.Reset)

		target, isFile := pointViperAtConfig([]string{"nested/missing.yml"})
		require.True(t, isFile, "a value naming a directory component is a path, not a name")
		require.Equal(t, "nested/missing.yml", target,
			"an unresolvable path is reported as the operator typed it")

		err := viper.ReadInConfig()
		require.Error(t, err)
		require.True(t, isConfigNotFound(err))

		msg := configNotFoundMessage(target, isFile)
		require.Contains(t, msg, "nested/missing.yml")
		require.Contains(t, msg, "search paths",
			"a relative path is resolved against them, so a miss that hides that is misleading")

		attrs := map[string]string{}
		for _, attr := range configLocationAttributes(target, isFile) {
			value, ok := attr.Value.(string)
			require.True(t, ok, "attribute %q must be a string, got %T", attr.Key, attr.Value)
			attrs[attr.Key] = value
		}
		require.Contains(t, attrs, "searched_paths")
		require.Contains(t, attrs["searched_paths"], filepath.Join("config", "nested", "missing.yml"),
			"the candidate list must name where the file was actually looked for")
		require.Contains(t, attrs, "working_directory",
			"a relative lookup is only decipherable alongside the directory it resolved against")
	})

	t.Run("missing bare name reports the search paths", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		viper.Reset()
		t.Cleanup(viper.Reset)

		target, isFile := pointViperAtConfig(nil)
		require.False(t, isFile)

		err := viper.ReadInConfig()
		require.Error(t, err)
		require.True(t, isConfigNotFound(err))

		require.Contains(t, configNotFoundMessage(target, isFile), "search paths")
		attrs := configLocationAttributes(target, isFile)
		var keys []string
		for _, attr := range attrs {
			keys = append(keys, attr.Key)
		}
		require.Contains(t, keys, "search_paths")
		require.Contains(t, keys, "working_directory",
			"a relative lookup is only decipherable alongside the directory it resolved against")
	})

	t.Run("a path that is a directory is a clean not-found, not a stack dump", func(t *testing.T) {
		// Pointing viper at a directory fails with EISDIR rather than fs.ErrNotExist, which
		// classified as "not a not-found" and so took the LavaFormatFatal path — the exact
		// failure mode MAG-2861 exists to remove, reintroduced in a narrow case. Searching
		// for a *name* never hit this: searchInPath skips directories.
		dir := t.TempDir()
		t.Chdir(dir)
		viper.Reset()
		t.Cleanup(viper.Reset)

		require.NoError(t, os.MkdirAll(filepath.Join(dir, "config"), 0o755))

		target, isFile := pointViperAtConfig([]string{"./config"})
		require.True(t, isFile)

		err := viper.ReadInConfig()
		require.Error(t, err)
		require.True(t, isConfigNotFound(err),
			"a directory holds no config, so it must stay on the clean path, got %v", err)
		require.Contains(t, configNotFoundMessage(target, isFile), "is a directory",
			"the message should say what is actually wrong rather than claim the path is absent")
	})

	t.Run("malformed config is not mistaken for a missing one", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		viper.Reset()
		t.Cleanup(viper.Reset)

		broken := filepath.Join(dir, "broken.yml")
		require.NoError(t, os.WriteFile(broken, []byte("this: [is not: valid\n"), 0o644))

		pointViperAtConfig([]string{broken})
		err := viper.ReadInConfig()
		require.Error(t, err)
		require.False(t, isConfigNotFound(err),
			"a parse failure must stay on the loud path — it is not an operator typo")
	})
}

// TestIsConfigFilePath pins the one judgement call in the resolution: which arguments are
// treated as paths. Getting this wrong either resurrects MAG-2861 or breaks the bare-name
// invocations that have always worked.
//
// The classification is deliberately lexical, so these cases hold regardless of what is on
// disk — the working directory below is populated precisely to prove that.
func TestIsConfigFilePath(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeConfig(t, filepath.Join(dir, "here.yml"), "x")
	writeConfig(t, filepath.Join(dir, "config", "there.yml"), "x")

	for _, tc := range []struct {
		arg  string
		want bool
		why  string
	}{
		{"/etc/smartrouter/config.yml", true, "an absolute path can never be resolved by the search paths"},
		{"/etc/smartrouter/missing.yml", true, "absolute stays a path even when absent, so the error can name it"},
		{"config/router.yml", true, "it names a directory component, so it is a path"},
		{"nested/missing.yml", true, "a path stays a path when absent, so the error can name it"},
		{"./router.yml", true, "an explicit ./ is intent"},
		{"here.yml", false, "no separator: a name, even though ./here.yml exists"},
		{"there.yml", false, "no separator: the search path's job"},
		{"myconfig", false, "a bare name is looked up, with the extension appended"},
		{"config", false, "no separator, so a name — the directory on disk is irrelevant"},
		{"", false, "no argument means the default name"},
	} {
		require.Equal(t, tc.want, isConfigFilePath(tc.arg), "%q: %s", tc.arg, tc.why)
	}
}

// TestConfigSearchPathsMatchDocumentedHelp keeps the command help honest: the Long text is
// the only place the search paths are written down, and both commands that take a config
// resolve it identically, so both have to say so. Documenting only rpcsmartrouter's left
// health's argument undescribed, and describing the paths as applying to a bare *name*
// left relative paths — which are resolved against them too — unaccounted for.
func TestConfigSearchPathsMatchDocumentedHelp(t *testing.T) {
	for name, long := range map[string]string{
		"rpcsmartrouter": CreateRPCSmartRouterCobraCommand().Long,
		"health":         CreateHealthCobraCommand().Long,
	} {
		t.Run(name, func(t *testing.T) {
			for _, path := range configSearchPaths {
				if path == "." {
					continue // spelled "the local running directory" in prose
				}
				require.True(t, strings.Contains(long, path),
					"search path %q is not mentioned in the command help", path)
			}
			require.Contains(t, long, "relative path",
				"the help must say a relative path is resolved against the search paths too")
		})
	}
}
