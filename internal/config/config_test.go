package config

import (
	"strings"
	"testing"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func validEnv() map[string]string {
	return map[string]string{
		"DATABASE_URL": "postgres://sierpe:s3cret-password@localhost:5432/sierpe",
		"NETWORK":      "testnet",
		"ADMIN_TOKEN":  "correct-horse-battery-staple",
	}
}

func TestLoadValid(t *testing.T) {
	cfg, err := Load(env(validEnv()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Network != NetworkTestnet {
		t.Errorf("Network = %q, want testnet", cfg.Network)
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want default 8080", cfg.HTTPPort)
	}
	if len(cfg.RPCURLs) == 0 {
		t.Error("RPCURLs empty, want testnet default")
	}
	if cfg.ArchiveEnabled() {
		t.Error("archive leg must be off without STELLAR_CORE_BINARY")
	}
}

func TestLoadArchiveLeg(t *testing.T) {
	m := validEnv()
	m["STELLAR_CORE_BINARY"] = "/bin/ls" // any executable file works for validation
	cfg, err := Load(env(m))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.ArchiveEnabled() {
		t.Fatal("archive leg must be on with STELLAR_CORE_BINARY set")
	}
	if len(cfg.ArchiveURLs) != 3 {
		t.Errorf("ArchiveURLs = %v, want the 3 testnet defaults", cfg.ArchiveURLs)
	}
	if cfg.CaptiveStoragePath == "" {
		t.Error("CaptiveStoragePath must default to the OS temp dir")
	}
	if !strings.Contains(cfg.Redacted(), "core=/bin/ls") {
		t.Errorf("Redacted() must show the archive config, got %s", cfg.Redacted())
	}
}

func TestLoadErrors(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantErr string
	}{
		{"missing database", func(m map[string]string) { delete(m, "DATABASE_URL") }, "DATABASE_URL is required"},
		{"bad database scheme", func(m map[string]string) { m["DATABASE_URL"] = "mysql://x" }, "postgres://"},
		{"missing network", func(m map[string]string) { delete(m, "NETWORK") }, "NETWORK is required"},
		{"unknown network", func(m map[string]string) { m["NETWORK"] = "futurenet" }, "not supported"},
		{"missing token", func(m map[string]string) { delete(m, "ADMIN_TOKEN") }, "ADMIN_TOKEN is required"},
		{"short token", func(m map[string]string) { m["ADMIN_TOKEN"] = "short" }, "too short"},
		{"repetitive token", func(m map[string]string) { m["ADMIN_TOKEN"] = strings.Repeat("ab", 10) }, "too repetitive"},
		{"mainnet needs rpc", func(m map[string]string) { m["NETWORK"] = "mainnet" }, "RPC_URLS is required on mainnet"},
		{"bad rpc url", func(m map[string]string) { m["RPC_URLS"] = "not a url" }, "not a valid http(s) URL"},
		{"bad port", func(m map[string]string) { m["HTTP_PORT"] = "99999" }, "not a valid port"},
		{"bad start ledger", func(m map[string]string) { m["START_LEDGER"] = "-4" }, "positive ledger sequence"},
		{"missing core binary", func(m map[string]string) { m["STELLAR_CORE_BINARY"] = "/no/such/binary" }, "does not exist"},
		{"core binary is a directory", func(m map[string]string) { m["STELLAR_CORE_BINARY"] = "/tmp" }, "not an executable file"},
		{"archives without core", func(m map[string]string) { m["HISTORY_ARCHIVE_URLS"] = "https://archives.example.com" }, "STELLAR_CORE_BINARY is not"},
		{"storage without core", func(m map[string]string) { m["CAPTIVE_STORAGE_PATH"] = "/var/captive" }, "STELLAR_CORE_BINARY is not"},
		{"bad archive url", func(m map[string]string) {
			m["STELLAR_CORE_BINARY"] = "/bin/ls"
			m["HISTORY_ARCHIVE_URLS"] = "not a url"
		}, "HISTORY_ARCHIVE_URLS entry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := validEnv()
			tc.mutate(m)
			_, err := Load(env(m))
			if err == nil {
				t.Fatal("Load() succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestRedactedLeaksNoSecrets(t *testing.T) {
	m := validEnv()
	m["RPC_URLS"] = "https://mainnet.stellar.validationcloud.io/v1/THE-API-KEY"
	cfg, err := Load(env(m))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	out := cfg.Redacted()
	for _, secret := range []string{"s3cret-password", "correct-horse-battery-staple", "THE-API-KEY"} {
		if strings.Contains(out, secret) {
			t.Errorf("Redacted() leaks %q: %s", secret, out)
		}
	}
	if !strings.Contains(out, "validationcloud.io") {
		t.Errorf("Redacted() should keep the RPC host for operability: %s", out)
	}
}

func TestNetworkPassphrase(t *testing.T) {
	if NetworkMainnet.Passphrase() != "Public Global Stellar Network ; September 2015" {
		t.Error("mainnet passphrase mismatch")
	}
	if NetworkTestnet.Passphrase() != "Test SDF Network ; September 2015" {
		t.Error("testnet passphrase mismatch")
	}
}

func TestLoadBasicAuth(t *testing.T) {
	m := validEnv()
	m["HTTP_BASIC_AUTH"] = "operator:correct-horse-battery"
	cfg, err := Load(env(m))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.BasicAuthEnabled() || cfg.BasicAuthUser != "operator" || cfg.BasicAuthPassword != "correct-horse-battery" {
		t.Errorf("basic auth = %q/%q enabled=%v", cfg.BasicAuthUser, cfg.BasicAuthPassword, cfg.BasicAuthEnabled())
	}
	red := cfg.Redacted()
	if strings.Contains(red, "correct-horse-battery") || strings.Contains(red, "operator") {
		t.Errorf("Redacted() leaks credentials: %s", red)
	}
	if !strings.Contains(red, "basic_auth=on") {
		t.Errorf("Redacted() must state basic auth is on: %s", red)
	}

	delete(m, "HTTP_BASIC_AUTH")
	cfg, err = Load(env(m))
	if err != nil || cfg.BasicAuthEnabled() {
		t.Errorf("basic auth must default off; err = %v", err)
	}

	for name, bad := range map[string]string{
		"no separator":   "operatorpassword",
		"empty user":     ":correct-horse-battery",
		"empty password": "operator:",
		"short password": "operator:short",
	} {
		m["HTTP_BASIC_AUTH"] = bad
		if _, err := Load(env(m)); err == nil {
			t.Errorf("%s (%q) must be rejected", name, bad)
		}
	}
}
