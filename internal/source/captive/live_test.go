package captive

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/source"
)

// TestLiveCaptiveReplay replays a small archived testnet range through a
// real captive stellar-core and verifies hash-chain continuity. Gated:
//
//	SIERPE_CAPTIVE_LIVE_TEST=1 go test ./internal/source/captive -run TestLive -v
//
// Requires a stellar-core binary (SIERPE_TEST_CORE_BINARY or stellar-core
// in PATH) and network access to the SDF testnet archives. Expect minutes,
// not seconds: core catches up from the nearest checkpoint.
func TestLiveCaptiveReplay(t *testing.T) {
	if os.Getenv("SIERPE_CAPTIVE_LIVE_TEST") != "1" {
		t.Skip("set SIERPE_CAPTIVE_LIVE_TEST=1 to run against real archives")
	}
	binary := os.Getenv("SIERPE_TEST_CORE_BINARY")
	if binary == "" {
		found, err := exec.LookPath("stellar-core")
		if err != nil {
			t.Skip("no stellar-core in PATH and SIERPE_TEST_CORE_BINARY unset")
		}
		binary = found
	}

	src, err := New(Config{
		BinaryPath: binary,
		Passphrase: network.TestNetworkPassphrase,
		ArchiveURLs: []string{
			"https://history.stellar.org/prd/core-testnet/core_testnet_001",
			"https://history.stellar.org/prd/core-testnet/core_testnet_002",
			"https://history.stellar.org/prd/core-testnet/core_testnet_003",
		},
		StoragePath: t.TempDir(),
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// A short range well below any RPC retention window but present in the
	// archives since the last testnet reset.
	const from, to = 4_100_000, 4_100_010
	var infos []source.Info
	err = src.ReplayRange(ctx, from, to, func(lcm xdr.LedgerCloseMeta) error {
		infos = append(infos, source.InfoOf(lcm))
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayRange() error = %v", err)
	}
	if len(infos) != to-from+1 {
		t.Fatalf("replayed %d ledgers, want %d", len(infos), to-from+1)
	}
	for i := 1; i < len(infos); i++ {
		if infos[i].PreviousHash != infos[i-1].Hash {
			t.Fatalf("hash discontinuity at %d", infos[i].Sequence)
		}
	}
	t.Logf("replayed [%d..%d] from archives, chain verified, closed_at %s",
		from, to, infos[0].ClosedAt)
}
