package licensing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadFromFile_EmptyPath(t *testing.T) {
	_, err := LoadFromFile("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestLoadFromFile_MissingFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.key")
	_, err := LoadFromFile(missing)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read license file")
	require.Contains(t, err.Error(), missing)
}

func TestLoadFromFile_EmptyContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "license.key")
	require.NoError(t, os.WriteFile(path, []byte("   \n  \t  "), 0o600))
	_, err := LoadFromFile(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is empty")
}

func TestLoadFromFile_ValidContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "license.key")
	contents := "some-license-envelope-base64-here\n"
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	envelope, err := LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, "some-license-envelope-base64-here", envelope)
}

func TestLoadFromEnvOrFile_EnvWins(t *testing.T) {
	envPath := filepath.Join(t.TempDir(), "env-license.key")
	fallbackPath := filepath.Join(t.TempDir(), "fallback.key")
	require.NoError(t, os.WriteFile(envPath, []byte("env-contents"), 0o600))
	require.NoError(t, os.WriteFile(fallbackPath, []byte("fallback-contents"), 0o600))

	t.Setenv("TEST_LICENSE_FILE", envPath)
	envelope, resolved, fromEnv, err := LoadFromEnvOrFile("TEST_LICENSE_FILE", fallbackPath)
	require.NoError(t, err)
	require.Equal(t, "env-contents", envelope)
	require.Equal(t, envPath, resolved)
	require.True(t, fromEnv)
}

func TestLoadFromEnvOrFile_FallsBackWhenEnvUnset(t *testing.T) {
	fallbackPath := filepath.Join(t.TempDir(), "fallback.key")
	require.NoError(t, os.WriteFile(fallbackPath, []byte("fallback-contents"), 0o600))

	// Explicitly setting to "" mimics `unset` for the precedence rule.
	t.Setenv("TEST_LICENSE_FILE", "")
	envelope, resolved, fromEnv, err := LoadFromEnvOrFile("TEST_LICENSE_FILE", fallbackPath)
	require.NoError(t, err)
	require.Equal(t, "fallback-contents", envelope)
	require.Equal(t, fallbackPath, resolved)
	require.False(t, fromEnv)
}

// Setting the env var to a non-existent file MUST fail — silent fallback to
// the default path would mask operator misconfiguration. The error message
// must mention the env path so the operator knows what to fix.
func TestLoadFromEnvOrFile_EnvPathMissingDoesNotFallBack(t *testing.T) {
	envPath := filepath.Join(t.TempDir(), "missing-env.key")
	fallbackPath := filepath.Join(t.TempDir(), "fallback.key")
	require.NoError(t, os.WriteFile(fallbackPath, []byte("fallback-contents"), 0o600))

	t.Setenv("TEST_LICENSE_FILE", envPath)
	_, resolved, fromEnv, err := LoadFromEnvOrFile("TEST_LICENSE_FILE", fallbackPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), envPath)
	require.Equal(t, envPath, resolved)
	require.True(t, fromEnv)
}

func TestLoadFromEnvOrFile_FallbackMissing(t *testing.T) {
	t.Setenv("TEST_LICENSE_FILE", "")
	missing := filepath.Join(t.TempDir(), "no-default.key")
	_, _, _, err := LoadFromEnvOrFile("TEST_LICENSE_FILE", missing)
	require.Error(t, err)
	require.Contains(t, err.Error(), missing)
}
