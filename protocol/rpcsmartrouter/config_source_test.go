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
			"an explicit path was never searched for — mentioning search paths is what sent MAG-2861 the wrong way")
	})

	t.Run("missing relative path is a clean not-found naming that path", func(t *testing.T) {
		// The whole point of MAG-2861 is that a path the operator named must not come back
		// as a complaint about directories they never mentioned. A relative path is no
		// less explicit than an absolute one.
		dir := t.TempDir()
		t.Chdir(dir)
		viper.Reset()
		t.Cleanup(viper.Reset)

		target, isFile := pointViperAtConfig([]string{"nested/missing.yml"})
		require.True(t, isFile, "a value naming a directory component is a path, not a name")
		require.Equal(t, "nested/missing.yml", target)

		err := viper.ReadInConfig()
		require.Error(t, err)
		require.True(t, isConfigNotFound(err))

		msg := configNotFoundMessage(target, isFile)
		require.Contains(t, msg, "nested/missing.yml")
		require.NotContains(t, msg, "search paths",
			"the search paths were never consulted for an explicit path — naming them is the MAG-2861 confusion")
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

// TestConfigSearchPathsMatchDocumentedHelp keeps the command help honest: the Long text
// tells operators which directories a bare name is looked up in, and it is the only place
// that claim is written down.
func TestConfigSearchPathsMatchDocumentedHelp(t *testing.T) {
	long := CreateRPCSmartRouterCobraCommand().Long
	for _, path := range configSearchPaths {
		if path == "." {
			continue // spelled "the local running directory" in prose
		}
		require.True(t, strings.Contains(long, path),
			"search path %q is not mentioned in the command help", path)
	}
}
