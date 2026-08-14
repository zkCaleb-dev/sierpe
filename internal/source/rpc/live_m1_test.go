package rpc

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/zkCaleb-dev/sierpe/internal/registry"
	"github.com/zkCaleb-dev/sierpe/internal/source"
)

// M1 live tests: the batch fetch path backfill depends on, and the
// classification pipeline, against the real public testnet. Gated exactly
// like TestLiveTestnet:
//
//	SIERPE_LIVE_TEST=1 go test ./internal/source/rpc -run TestLive -v

// Live fixtures on testnet: the OpenZeppelin Confidential Token (a wasm
// contract with declared spec events) and the native XLM SAC.
const (
	liveWasmContract = "CBF64DEOVQAXJFBSNGFEUT2AH4H7K5JBY3ZYJ5GVEINMNSDISWRG5N3F"
	liveSACContract  = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"
)

func liveClient(t *testing.T) (*Client, context.Context) {
	t.Helper()
	if os.Getenv("SIERPE_LIVE_TEST") != "1" {
		t.Skip("set SIERPE_LIVE_TEST=1 to run against the public testnet RPC")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	c, err := New([]string{"https://soroban-testnet.stellar.org"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, ctx
}

// TestLiveLedgerBatch pins the backfill fetch path: a real batch decodes
// and chains hash to hash.
func TestLiveLedgerBatch(t *testing.T) {
	c, ctx := liveClient(t)

	latest, err := c.LatestLedger(ctx)
	if err != nil {
		t.Fatalf("LatestLedger() error = %v", err)
	}
	start := latest - 20
	batch, err := c.GetLedgerBatch(ctx, start, 10)
	if err != nil {
		t.Fatalf("GetLedgerBatch(%d,10) error = %v", start, err)
	}
	if len(batch) == 0 {
		t.Fatal("empty batch inside the retention window")
	}
	prev := ""
	for i, lcm := range batch {
		info := source.InfoOf(lcm)
		if info.Sequence != start+uint32(i) {
			t.Fatalf("batch[%d] sequence = %d, want %d", i, info.Sequence, start+uint32(i))
		}
		if prev != "" && info.PreviousHash != prev {
			t.Fatalf("batch discontinuity at %d", info.Sequence)
		}
		prev = info.Hash
	}
	t.Logf("batch of %d ledgers from %d chained cleanly", len(batch), start)
}

// TestLiveClassification pins the registration pipeline against real
// on-chain data: a wasm contract with declared spec events, a SAC by
// executable, and the not-found path.
func TestLiveClassification(t *testing.T) {
	c, ctx := liveClient(t)
	classifier := registry.NewClassifier(c)

	wasm, err := classifier.Classify(ctx, liveWasmContract)
	if err != nil {
		t.Fatalf("Classify(wasm) error = %v", err)
	}
	if wasm.Type != registry.TypeWasm || wasm.Method != registry.MethodSpecEvents {
		t.Errorf("wasm classification = %+v, want wasm/spec_events", wasm)
	}
	if len(wasm.Events) == 0 || wasm.WasmHash == "" {
		t.Errorf("wasm classification incomplete: %+v", wasm)
	}
	hasTransfer := false
	for _, ev := range wasm.Events {
		if ev == "transfer" {
			hasTransfer = true
		}
	}
	if !hasTransfer {
		t.Errorf("the OZ confidential token must declare a transfer event, got %v", wasm.Events)
	}
	t.Logf("wasm contract declares %d events via %s", len(wasm.Events), wasm.Method)

	sac, err := classifier.Classify(ctx, liveSACContract)
	if err != nil {
		t.Fatalf("Classify(sac) error = %v", err)
	}
	if sac.Type != registry.TypeSAC || sac.Method != registry.MethodSACBuiltin {
		t.Errorf("sac classification = %+v, want sac/sac_builtin", sac)
	}

	// A syntactically valid contract id that was never deployed.
	if _, err := classifier.Classify(ctx, "CA3D5KRYM6CB7OWQ6TWYRR3Z4T7GNZLKERYNZGGA5SOAOPIFY6YQGAXE"); !errors.Is(err, registry.ErrContractNotFound) {
		t.Errorf("Classify(undeployed) = %v, want ErrContractNotFound", err)
	}
}
