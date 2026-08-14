package rpc

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/source"
)

// TestLiveTestnet exercises the client against the real public testnet RPC.
// Gated behind SIERPE_LIVE_TEST=1 so CI and offline runs stay deterministic.
//
//	SIERPE_LIVE_TEST=1 go test ./internal/source/rpc -run TestLive -v
func TestLiveTestnet(t *testing.T) {
	if os.Getenv("SIERPE_LIVE_TEST") != "1" {
		t.Skip("set SIERPE_LIVE_TEST=1 to run against the public testnet RPC")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c, err := New([]string{"https://soroban-testnet.stellar.org"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	if err := c.VerifyNetwork(ctx, "Test SDF Network ; September 2015"); err != nil {
		t.Fatalf("VerifyNetwork() error = %v", err)
	}

	latest, err := c.LatestLedger(ctx)
	if err != nil {
		t.Fatalf("LatestLedger() error = %v", err)
	}
	if latest == 0 {
		t.Fatal("LatestLedger() = 0")
	}
	t.Logf("testnet tip: %d", latest)

	// A recent-but-closed ledger must decode with a coherent header chain.
	lcm, err := c.GetLedger(ctx, latest-2)
	if err != nil {
		t.Fatalf("GetLedger(%d) error = %v", latest-2, err)
	}
	info := source.InfoOf(lcm)
	if info.Sequence != latest-2 {
		t.Errorf("InfoOf sequence = %d, want %d", info.Sequence, latest-2)
	}
	if len(info.Hash) != 64 || len(info.PreviousHash) != 64 {
		t.Errorf("hashes not 64-hex: hash=%q prev=%q", info.Hash, info.PreviousHash)
	}
	if info.ClosedAt.Before(time.Now().Add(-time.Hour)) {
		t.Errorf("ClosedAt %v is implausibly old for a tip-adjacent ledger", info.ClosedAt)
	}

	// Continuity across two adjacent ledgers: prev hash must chain.
	next, err := c.GetLedger(ctx, latest-1)
	if err != nil {
		t.Fatalf("GetLedger(%d) error = %v", latest-1, err)
	}
	nextInfo := source.InfoOf(next)
	if nextInfo.PreviousHash != info.Hash {
		t.Errorf("chain mismatch: ledger %d prev=%s, ledger %d hash=%s",
			nextInfo.Sequence, nextInfo.PreviousHash, info.Sequence, info.Hash)
	}

	// A far-future ledger classifies as not-yet-available.
	if _, err := c.GetLedger(ctx, latest+1_000_000); err == nil {
		t.Error("GetLedger(far future) succeeded, want ErrNotYetAvailable")
	} else if !errors.Is(err, source.ErrNotYetAvailable) {
		t.Errorf("GetLedger(far future) = %v, want ErrNotYetAvailable", err)
	}
}
