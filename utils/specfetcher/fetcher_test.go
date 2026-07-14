package specfetcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseRepoURL_GitHub(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantProject string
		wantBranch  string
		wantPath    string
		wantError   bool
	}{
		{
			name:        "Valid GitHub URL with path",
			input:       "https://github.com/magma-Devs/smart-router-specs/tree/main/specs/mainnet-1/specs",
			wantProject: "magma-Devs/smart-router-specs",
			wantBranch:  "main",
			wantPath:    "specs/mainnet-1/specs",
			wantError:   false,
		},
		{
			name:        "Valid GitHub URL without path",
			input:       "https://github.com/magma-devs/lava-specs/tree/main",
			wantProject: "magma-devs/lava-specs",
			wantBranch:  "main",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Valid GitHub URL with trailing slash",
			input:       "https://github.com/user/repo/tree/develop/",
			wantProject: "user/repo",
			wantBranch:  "develop",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Bare repo URL - default branch",
			input:       "https://github.com/user/repo",
			wantProject: "user/repo",
			wantBranch:  "",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Bare repo URL with trailing slash",
			input:       "https://github.com/magma-devs/lava-specs/",
			wantProject: "magma-devs/lava-specs",
			wantBranch:  "",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Codeload tarball URL - refs/heads",
			input:       "https://codeload.github.com/Magma-Devs/lava-specs/tar.gz/refs/heads/main",
			wantProject: "Magma-Devs/lava-specs",
			wantBranch:  "main",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Codeload tarball URL - refs/tags",
			input:       "https://codeload.github.com/user/repo/tar.gz/refs/tags/v1.2.3",
			wantProject: "user/repo",
			wantBranch:  "v1.2.3",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Codeload tarball URL - HEAD",
			input:       "https://codeload.github.com/user/repo/tar.gz/HEAD",
			wantProject: "user/repo",
			wantBranch:  "",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Codeload tarball URL - bare ref",
			input:       "https://codeload.github.com/user/repo/tar.gz/develop",
			wantProject: "user/repo",
			wantBranch:  "develop",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Codeload tarball URL - branch with slash",
			input:       "https://codeload.github.com/user/repo/tar.gz/refs/heads/release/v1",
			wantProject: "user/repo",
			wantBranch:  "release/v1",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:      "Invalid GitHub URL - wrong format",
			input:     "https://github.com/invalid/url/blob/main/file.go",
			wantError: true,
		},
		{
			name:      "Invalid GitHub URL - owner only",
			input:     "https://github.com/user",
			wantError: true,
		},
		{
			name:      "Invalid codeload URL - no ref",
			input:     "https://codeload.github.com/user/repo/tar.gz",
			wantError: true,
		},
		{
			name:      "Invalid codeload URL - zip archive",
			input:     "https://codeload.github.com/user/repo/zip/refs/heads/main",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseRepoURL(tt.input)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, ProviderGitHub, info.Provider)
			require.Equal(t, tt.wantProject, info.ProjectPath)
			require.Equal(t, tt.wantBranch, info.Branch)
			require.Equal(t, tt.wantPath, info.Path)
		})
	}
}

func TestParseRepoURL_GitLab(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantHost    string
		wantProject string
		wantBranch  string
		wantPath    string
		wantError   bool
	}{
		{
			name:        "Valid GitLab URL with path",
			input:       "https://gitlab.com/myorg/myrepo/-/tree/main/specs",
			wantHost:    "https://gitlab.com",
			wantProject: "myorg/myrepo",
			wantBranch:  "main",
			wantPath:    "specs",
			wantError:   false,
		},
		{
			name:        "Valid GitLab URL without path",
			input:       "https://gitlab.com/group/repo/-/tree/develop",
			wantHost:    "https://gitlab.com",
			wantProject: "group/repo",
			wantBranch:  "develop",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Valid GitLab URL with nested group",
			input:       "https://gitlab.com/org/subgroup/repo/-/tree/main/path/to/specs",
			wantHost:    "https://gitlab.com",
			wantProject: "org/subgroup/repo",
			wantBranch:  "main",
			wantPath:    "path/to/specs",
			wantError:   false,
		},
		{
			name:        "Self-hosted GitLab",
			input:       "https://gitlab.mycompany.com/team/project/-/tree/release/v1",
			wantHost:    "https://gitlab.mycompany.com",
			wantProject: "team/project",
			wantBranch:  "release",
			wantPath:    "v1",
			wantError:   false,
		},
		{
			name:        "Bare gitlab.com repo URL - default branch",
			input:       "https://gitlab.com/myorg/myrepo",
			wantHost:    "https://gitlab.com",
			wantProject: "myorg/myrepo",
			wantBranch:  "",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Bare gitlab.com nested-group URL",
			input:       "https://gitlab.com/org/subgroup/repo/",
			wantHost:    "https://gitlab.com",
			wantProject: "org/subgroup/repo",
			wantBranch:  "",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Archive tarball URL",
			input:       "https://gitlab.com/myorg/myrepo/-/archive/main/myrepo-main.tar.gz",
			wantHost:    "https://gitlab.com",
			wantProject: "myorg/myrepo",
			wantBranch:  "main",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Archive tarball URL with slashed ref",
			input:       "https://gitlab.example.com/team/project/-/archive/release/v1/project-release-v1.tar.gz",
			wantHost:    "https://gitlab.example.com",
			wantProject: "team/project",
			wantBranch:  "release/v1",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:        "Archive tarball URL - HEAD",
			input:       "https://gitlab.com/myorg/myrepo/-/archive/HEAD/myrepo-HEAD.tar.gz",
			wantHost:    "https://gitlab.com",
			wantProject: "myorg/myrepo",
			wantBranch:  "",
			wantPath:    "",
			wantError:   false,
		},
		{
			name:      "Invalid GitLab URL - no dash separator",
			input:     "https://gitlab.com/user/repo/tree/main",
			wantError: true,
		},
		{
			name:      "Invalid archive URL - not tar.gz",
			input:     "https://gitlab.com/myorg/myrepo/-/archive/main/myrepo-main.zip",
			wantError: true,
		},
		{
			name:      "Invalid bare gitlab.com URL - owner only",
			input:     "https://gitlab.com/user",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseRepoURL(tt.input)

			if tt.wantError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, ProviderGitLab, info.Provider)
			require.Equal(t, tt.wantHost, info.Host)
			require.Equal(t, tt.wantProject, info.ProjectPath)
			require.Equal(t, tt.wantBranch, info.Branch)
			require.Equal(t, tt.wantPath, info.Path)
		})
	}
}

func TestIsGitHubURL(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"https://github.com/user/repo/tree/main", true},
		{"https://github.com/org/project/tree/develop/path", true},
		{"https://github.com/user/repo", true},
		{"https://github.com/user/repo/", true},
		{"https://codeload.github.com/user/repo/tar.gz/refs/heads/main", true},
		{"https://gitlab.com/user/repo/-/tree/main", false},
		{"https://gitlab.mycompany.com/team/repo/-/tree/main", false},
		{"./local/path", false},
		{"/absolute/path", false},
		{"invalid-url", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsGitHubURL(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsGitLabURL(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"https://gitlab.com/user/repo/-/tree/main", true},
		{"https://gitlab.mycompany.com/team/repo/-/tree/develop", true},
		{"https://gitlab.example.org/group/subgroup/repo/-/tree/main/path", true},
		{"https://gitlab.com/user/repo", true},
		{"https://gitlab.com/myorg/myrepo/-/archive/main/myrepo-main.tar.gz", true},
		{"https://github.com/user/repo/tree/main", false},
		{"https://github.com/user/repo", false},
		{"./local/path", false},
		{"/absolute/path", false},
		{"invalid-url", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsGitLabURL(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIsRemoteRepoURL(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"https://github.com/user/repo/tree/main", true},
		{"https://github.com/user/repo/", true},
		{"https://codeload.github.com/user/repo/tar.gz/refs/heads/main", true},
		{"https://gitlab.com/user/repo/-/tree/main", true},
		{"https://gitlab.mycompany.com/team/repo/-/tree/develop", true},
		{"./local/path", false},
		{"/absolute/path", false},
		{"https://example.com/not-a-repo", false},
		{"invalid-url", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsRemoteRepoURL(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	require.Equal(t, DefaultAPITimeout, config.APITimeout)
	require.Equal(t, DefaultFileFetchTimeout, config.FileFetchTimeout)
	require.Equal(t, DefaultMaxConcurrency, config.MaxConcurrency)
	require.NotNil(t, config.HTTPClient)
}

func TestNew(t *testing.T) {
	t.Run("with defaults", func(t *testing.T) {
		fetcher := New(Config{})

		require.Equal(t, DefaultAPITimeout, fetcher.config.APITimeout)
		require.Equal(t, DefaultFileFetchTimeout, fetcher.config.FileFetchTimeout)
		require.Equal(t, DefaultMaxConcurrency, fetcher.config.MaxConcurrency)
	})

	t.Run("with custom config", func(t *testing.T) {
		config := Config{
			Token:            "test-token",
			APITimeout:       10 * DefaultAPITimeout,
			FileFetchTimeout: 10 * DefaultFileFetchTimeout,
			MaxConcurrency:   5,
		}
		fetcher := New(config)

		require.Equal(t, "test-token", fetcher.config.Token)
		require.Equal(t, config.APITimeout, fetcher.config.APITimeout)
		require.Equal(t, config.FileFetchTimeout, fetcher.config.FileFetchTimeout)
		require.Equal(t, 5, fetcher.config.MaxConcurrency)
	})
}

// Benchmark tests
func BenchmarkParseRepoURL_GitHub(b *testing.B) {
	url := "https://github.com/magma-Devs/smart-router-specs/tree/main/specs/mainnet-1/specs"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseRepoURL(url)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseRepoURL_GitLab(b *testing.B) {
	url := "https://gitlab.com/myorg/myrepo/-/tree/main/specs"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ParseRepoURL(url)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIsRemoteRepoURL(b *testing.B) {
	urls := []string{
		"https://github.com/user/repo/tree/main",
		"https://gitlab.com/user/repo/-/tree/main",
		"./local/path",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, url := range urls {
			_ = IsRemoteRepoURL(url)
		}
	}
}
