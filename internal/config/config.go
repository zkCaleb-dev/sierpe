// Package config loads, validates, and redacts Sierpe's boot configuration.
//
// Boot config comes from environment variables and requires a restart to
// change. Hot config (registered contracts, retention) lives in the database
// and is managed through the admin API. See docs/DESIGN.md §7.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Network identifies the Stellar network this instance indexes.
type Network string

const (
	NetworkTestnet Network = "testnet"
	NetworkMainnet Network = "mainnet"
)

// Passphrase returns the network passphrase used to verify RPC endpoints.
func (n Network) Passphrase() string {
	switch n {
	case NetworkMainnet:
		return "Public Global Stellar Network ; September 2015"
	case NetworkTestnet:
		return "Test SDF Network ; September 2015"
	default:
		return ""
	}
}

// defaultRPCURLs holds the public endpoints used when RPC_URLS is not set.
// Mainnet has no reliable free default; it must be configured explicitly.
var defaultRPCURLs = map[Network][]string{
	NetworkTestnet: {"https://soroban-testnet.stellar.org"},
}

// defaultArchiveURLs holds the SDF public history archives used when
// HISTORY_ARCHIVE_URLS is not set. Unlike RPC these are static file
// hosting; the public defaults are reliable on both networks.
var defaultArchiveURLs = map[Network][]string{
	NetworkTestnet: {
		"https://history.stellar.org/prd/core-testnet/core_testnet_001",
		"https://history.stellar.org/prd/core-testnet/core_testnet_002",
		"https://history.stellar.org/prd/core-testnet/core_testnet_003",
	},
	NetworkMainnet: {
		"https://history.stellar.org/prd/core-live/core_live_001",
		"https://history.stellar.org/prd/core-live/core_live_002",
		"https://history.stellar.org/prd/core-live/core_live_003",
	},
}

// Config is Sierpe's validated boot configuration.
type Config struct {
	// DatabaseURL points at an empty (or Sierpe-owned) Postgres database.
	// Sierpe owns the schema inside it and runs its own migrations.
	DatabaseURL string
	// Network is the Stellar network to index.
	Network Network
	// AdminToken gates the admin API. Enforced minimum entropy at boot.
	AdminToken string
	// RPCURLs is the failover pool of Stellar RPC endpoints, in order.
	RPCURLs []string
	// HTTPPort is where the API and health endpoints listen.
	HTTPPort int
	// StartLedger optionally pins the first ledger to ingest when no cursor
	// exists yet. Zero means "start at the current tip".
	StartLedger uint32
	// CoreBinary is the path to a stellar-core binary. Setting it enables
	// the archive leg (captive core replay of history below RPC retention).
	// Empty means the archive leg is off and gaps stay recorded.
	CoreBinary string
	// ArchiveURLs are the history archives captive core replays from.
	// Only meaningful when CoreBinary is set.
	ArchiveURLs []string
	// CaptiveStoragePath is where captive core keeps its bucket data.
	// Contents are disposable (re-downloaded on demand).
	CaptiveStoragePath string
	// BasicAuthUser and BasicAuthPassword gate the ENTIRE http surface
	// (UI included) behind Basic Auth when set, except /health and /ready
	// which orchestrator probes must reach without credentials. Empty
	// means the open-reads model: use it behind private networking.
	BasicAuthUser     string
	BasicAuthPassword string
}

// BasicAuthEnabled reports whether the whole-surface gate is configured.
func (c *Config) BasicAuthEnabled() bool { return c.BasicAuthUser != "" }

// ArchiveEnabled reports whether the archive leg is configured.
func (c *Config) ArchiveEnabled() bool { return c.CoreBinary != "" }

// Load reads configuration from lookup (normally os.LookupEnv), applies
// defaults, and validates everything. It returns actionable errors: a config
// that loads is a config the process can run with.
func Load(lookup func(string) (string, bool)) (*Config, error) {
	var errs []string

	cfg := &Config{}

	dbURL, _ := lookup("DATABASE_URL")
	if dbURL == "" {
		errs = append(errs, "DATABASE_URL is required (a Postgres connection string; Sierpe owns the schema)")
	} else if u, err := url.Parse(dbURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		errs = append(errs, "DATABASE_URL must be a postgres:// or postgresql:// URL")
	}
	cfg.DatabaseURL = dbURL

	network, _ := lookup("NETWORK")
	switch Network(network) {
	case NetworkTestnet, NetworkMainnet:
		cfg.Network = Network(network)
	case "":
		errs = append(errs, "NETWORK is required (testnet or mainnet)")
	default:
		errs = append(errs, fmt.Sprintf("NETWORK %q is not supported (use testnet or mainnet)", network))
	}

	token, _ := lookup("ADMIN_TOKEN")
	if err := validateAdminToken(token); err != nil {
		errs = append(errs, err.Error())
	}
	cfg.AdminToken = token

	rawURLs, _ := lookup("RPC_URLS")
	cfg.RPCURLs = splitURLs(rawURLs)
	if len(cfg.RPCURLs) == 0 {
		cfg.RPCURLs = defaultRPCURLs[cfg.Network]
	}
	if len(cfg.RPCURLs) == 0 && cfg.Network != "" {
		errs = append(errs, "RPC_URLS is required on mainnet (comma-separated failover pool; there is no default public mainnet RPC)")
	}
	for _, raw := range cfg.RPCURLs {
		if u, err := url.Parse(raw); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Sprintf("RPC_URLS entry %q is not a valid http(s) URL", raw))
		}
	}

	cfg.HTTPPort = 8080
	if port, ok := lookup("HTTP_PORT"); ok && port != "" {
		p, err := strconv.Atoi(port)
		if err != nil || p < 1 || p > 65535 {
			errs = append(errs, fmt.Sprintf("HTTP_PORT %q is not a valid port", port))
		} else {
			cfg.HTTPPort = p
		}
	}

	if raw, ok := lookup("START_LEDGER"); ok && raw != "" {
		v, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || v == 0 {
			errs = append(errs, fmt.Sprintf("START_LEDGER %q must be a positive ledger sequence", raw))
		} else {
			cfg.StartLedger = uint32(v)
		}
	}

	if bin, ok := lookup("STELLAR_CORE_BINARY"); ok && bin != "" {
		info, err := os.Stat(bin)
		switch {
		case err != nil:
			errs = append(errs, fmt.Sprintf("STELLAR_CORE_BINARY %q does not exist (%v)", bin, err))
		case info.IsDir() || info.Mode().Perm()&0o111 == 0:
			errs = append(errs, fmt.Sprintf("STELLAR_CORE_BINARY %q is not an executable file", bin))
		default:
			cfg.CoreBinary = bin
		}
	}
	rawArchives, archivesSet := lookup("HISTORY_ARCHIVE_URLS")
	cfg.ArchiveURLs = splitURLs(rawArchives)
	if len(cfg.ArchiveURLs) == 0 {
		cfg.ArchiveURLs = defaultArchiveURLs[cfg.Network]
	}
	for _, raw := range cfg.ArchiveURLs {
		if u, err := url.Parse(raw); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Sprintf("HISTORY_ARCHIVE_URLS entry %q is not a valid http(s) URL", raw))
		}
	}
	if archivesSet && rawArchives != "" && cfg.CoreBinary == "" {
		// A configured variable that does nothing is a lie (rule 12).
		errs = append(errs, "HISTORY_ARCHIVE_URLS is set but STELLAR_CORE_BINARY is not; the archive leg needs both")
	}
	cfg.CaptiveStoragePath = os.TempDir()
	if p, ok := lookup("CAPTIVE_STORAGE_PATH"); ok && p != "" {
		if cfg.CoreBinary == "" {
			errs = append(errs, "CAPTIVE_STORAGE_PATH is set but STELLAR_CORE_BINARY is not; the archive leg needs both")
		}
		cfg.CaptiveStoragePath = p
	}

	if raw, ok := lookup("HTTP_BASIC_AUTH"); ok && raw != "" {
		user, pass, found := strings.Cut(raw, ":")
		switch {
		case !found || user == "" || pass == "":
			errs = append(errs, "HTTP_BASIC_AUTH must be user:password (both non-empty)")
		case len(pass) < 12:
			errs = append(errs, "HTTP_BASIC_AUTH password is too short (min 12 chars; it gates the whole surface)")
		default:
			cfg.BasicAuthUser, cfg.BasicAuthPassword = user, pass
		}
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}
	return cfg, nil
}

// validateAdminToken enforces a minimum entropy so a forgotten default or a
// trivial token cannot expose the admin surface.
func validateAdminToken(token string) error {
	const minLen, minDistinct = 16, 6
	if token == "" {
		return fmt.Errorf("ADMIN_TOKEN is required (min %d chars; it gates the admin API)", minLen)
	}
	if len(token) < minLen {
		return fmt.Errorf("ADMIN_TOKEN is too short (min %d chars)", minLen)
	}
	distinct := map[rune]struct{}{}
	for _, r := range token {
		distinct[r] = struct{}{}
	}
	if len(distinct) < minDistinct {
		return fmt.Errorf("ADMIN_TOKEN is too repetitive (needs at least %d distinct characters)", minDistinct)
	}
	return nil
}

func splitURLs(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Redacted renders the configuration for logs with every secret masked:
// the admin token, the database password, and RPC URL paths (providers such
// as Validation Cloud embed API keys in the path).
func (c *Config) Redacted() string {
	db := c.DatabaseURL
	if u, err := url.Parse(db); err == nil {
		db = u.Redacted()
	}
	hosts := make([]string, 0, len(c.RPCURLs))
	for _, raw := range c.RPCURLs {
		if u, err := url.Parse(raw); err == nil {
			hosts = append(hosts, u.Scheme+"://"+u.Host)
		} else {
			hosts = append(hosts, "<invalid>")
		}
	}
	archive := "off"
	if c.ArchiveEnabled() {
		archiveHosts := make([]string, 0, len(c.ArchiveURLs))
		for _, raw := range c.ArchiveURLs {
			if u, err := url.Parse(raw); err == nil {
				archiveHosts = append(archiveHosts, u.Scheme+"://"+u.Host)
			} else {
				archiveHosts = append(archiveHosts, "<invalid>")
			}
		}
		archive = fmt.Sprintf("core=%s archives=[%s]", c.CoreBinary, strings.Join(archiveHosts, " "))
	}
	basicAuth := "off"
	if c.BasicAuthEnabled() {
		basicAuth = "on"
	}
	return fmt.Sprintf(
		"network=%s database=%s rpc=[%s] http_port=%d start_ledger=%d archive=%s basic_auth=%s admin_token=<set>",
		c.Network, db, strings.Join(hosts, " "), c.HTTPPort, c.StartLedger, archive, basicAuth,
	)
}
