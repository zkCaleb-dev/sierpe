// Package rpc implements source.Source over the Stellar RPC JSON-RPC API
// with an ordered failover pool.
//
// Every request carries a per-attempt deadline; an endpoint that fails hands
// over to the next one in the pool (CLAUDE.md rule 9, KNOWLEDGE.md P3). The
// pool remembers the last healthy endpoint to avoid re-walking dead ones on
// every call.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/zkCaleb-dev/sierpe/internal/source"
)

const (
	attemptTimeout = 30 * time.Second
	maxBodyBytes   = 64 << 20 // a single mainnet ledger's meta can be large
)

// Client is a failover pool of Stellar RPC endpoints.
type Client struct {
	urls      []string
	http      *http.Client
	preferred atomic.Int32 // index of the last endpoint that answered
	failovers atomic.Int64
}

// New builds a Client over the given endpoint pool. The pool must not be
// empty; order expresses preference.
func New(urls []string) (*Client, error) {
	if len(urls) == 0 {
		return nil, errors.New("rpc: endpoint pool is empty")
	}
	return &Client{
		urls: urls,
		http: &http.Client{Timeout: attemptTimeout},
	}, nil
}

// Failovers reports how many times the pool had to switch endpoints.
func (c *Client) Failovers() int64 { return c.failovers.Load() }

// Close implements source.Source.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// LatestLedger implements source.Source.
func (c *Client) LatestLedger(ctx context.Context) (uint32, error) {
	var res struct {
		Sequence uint32 `json:"sequence"`
	}
	if err := c.call(ctx, "getLatestLedger", nil, &res); err != nil {
		return 0, fmt.Errorf("getLatestLedger: %w", err)
	}
	return res.Sequence, nil
}

// VerifyNetwork implements source.Source.
func (c *Client) VerifyNetwork(ctx context.Context, passphrase string) error {
	var res struct {
		Passphrase string `json:"passphrase"`
	}
	if err := c.call(ctx, "getNetwork", nil, &res); err != nil {
		return fmt.Errorf("getNetwork: %w", err)
	}
	if res.Passphrase != passphrase {
		return fmt.Errorf("rpc serves network %q, this instance is configured for %q — refusing to mix networks", res.Passphrase, passphrase)
	}
	return nil
}

// GetLedger implements source.Source.
func (c *Client) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	params := map[string]any{
		"startLedger": seq,
		"pagination":  map[string]any{"limit": 1},
	}
	var res struct {
		Ledgers []struct {
			Sequence    uint32 `json:"sequence"`
			MetadataXDR string `json:"metadataXdr"`
		} `json:"ledgers"`
		LatestLedger uint32 `json:"latestLedger"`
		OldestLedger uint32 `json:"oldestLedger"`
	}
	var lcm xdr.LedgerCloseMeta

	err := c.call(ctx, "getLedgers", params, &res)
	if err != nil {
		// The RPC reports out-of-range requests as errors; classify them by
		// what the source can actually serve (never trust getHealth).
		var rpcErr *jsonRPCError
		if errors.As(err, &rpcErr) && rpcErr.looksOutOfRange() {
			if err := c.classifyOutOfRange(ctx, seq); err != nil {
				return lcm, err
			}
		}
		return lcm, fmt.Errorf("getLedgers(%d): %w", seq, err)
	}

	if len(res.Ledgers) == 0 || res.Ledgers[0].Sequence != seq {
		if seq > res.LatestLedger {
			return lcm, fmt.Errorf("ledger %d beyond tip %d: %w", seq, res.LatestLedger, source.ErrNotYetAvailable)
		}
		if seq < res.OldestLedger {
			return lcm, fmt.Errorf("ledger %d below oldest %d: %w", seq, res.OldestLedger, source.ErrBelowRetention)
		}
		return lcm, fmt.Errorf("getLedgers(%d): empty result inside claimed window [%d,%d]", seq, res.OldestLedger, res.LatestLedger)
	}

	if err := xdr.SafeUnmarshalBase64(res.Ledgers[0].MetadataXDR, &lcm); err != nil {
		return lcm, fmt.Errorf("decoding ledger %d meta: %w", seq, err)
	}
	return lcm, nil
}

// classifyOutOfRange decides what a window error means by probing the tip.
// A failed probe returns nil so the caller reports the original error as
// transient: fatally classifying below-retention on a flaky probe would
// kill the process over a hiccup (the M0 bug this replaces did exactly
// that under backfill load).
func (c *Client) classifyOutOfRange(ctx context.Context, seq uint32) error {
	latest, err := c.LatestLedger(ctx)
	if err != nil {
		return nil
	}
	if seq > latest {
		return fmt.Errorf("ledger %d beyond tip %d: %w", seq, latest, source.ErrNotYetAvailable)
	}
	return fmt.Errorf("ledger %d: %w", seq, source.ErrBelowRetention)
}

// maxBatchLimit is the getLedgers pagination cap enforced by Stellar RPC.
const maxBatchLimit = 200

// GetLedgerBatch fetches up to limit consecutive ledgers ascending from
// start. It returns what the endpoint served (possibly fewer than limit);
// window errors classify exactly like GetLedger.
func (c *Client) GetLedgerBatch(ctx context.Context, start uint32, limit int) ([]xdr.LedgerCloseMeta, error) {
	if limit > maxBatchLimit {
		limit = maxBatchLimit
	}
	params := map[string]any{
		"startLedger": start,
		"pagination":  map[string]any{"limit": limit},
	}
	var res struct {
		Ledgers []struct {
			Sequence    uint32 `json:"sequence"`
			MetadataXDR string `json:"metadataXdr"`
		} `json:"ledgers"`
		LatestLedger uint32 `json:"latestLedger"`
		OldestLedger uint32 `json:"oldestLedger"`
	}
	if err := c.call(ctx, "getLedgers", params, &res); err != nil {
		var rpcErr *jsonRPCError
		if errors.As(err, &rpcErr) && rpcErr.looksOutOfRange() {
			if err := c.classifyOutOfRange(ctx, start); err != nil {
				return nil, err
			}
		}
		return nil, fmt.Errorf("getLedgers(%d,+%d): %w", start, limit, err)
	}
	if len(res.Ledgers) == 0 {
		if start > res.LatestLedger {
			return nil, fmt.Errorf("ledger %d beyond tip %d: %w", start, res.LatestLedger, source.ErrNotYetAvailable)
		}
		if start < res.OldestLedger {
			return nil, fmt.Errorf("ledger %d below oldest %d: %w", start, res.OldestLedger, source.ErrBelowRetention)
		}
		return nil, fmt.Errorf("getLedgers(%d,+%d): empty result inside claimed window [%d,%d]", start, limit, res.OldestLedger, res.LatestLedger)
	}

	out := make([]xdr.LedgerCloseMeta, 0, len(res.Ledgers))
	for _, l := range res.Ledgers {
		var lcm xdr.LedgerCloseMeta
		if err := xdr.SafeUnmarshalBase64(l.MetadataXDR, &lcm); err != nil {
			return nil, fmt.Errorf("decoding ledger %d meta: %w", l.Sequence, err)
		}
		out = append(out, lcm)
	}
	return out, nil
}

// GetLedgerEntry fetches one current ledger entry by its base64 XDR key.
// found=false means the entry does not exist on chain — which is an answer,
// not an error (the caller decides what absence means).
func (c *Client) GetLedgerEntry(ctx context.Context, keyB64 string) (string, bool, error) {
	params := map[string]any{"keys": []string{keyB64}}
	var res struct {
		Entries []struct {
			Xdr string `json:"xdr"`
		} `json:"entries"`
	}
	if err := c.call(ctx, "getLedgerEntries", params, &res); err != nil {
		return "", false, fmt.Errorf("getLedgerEntries: %w", err)
	}
	if len(res.Entries) == 0 {
		return "", false, nil
	}
	return res.Entries[0].Xdr, true, nil
}

// --- JSON-RPC plumbing ---

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *jsonRPCError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

// looksOutOfRange matches the RPC's window errors. The wording differs
// across implementations, so match conservatively on both code and message.
func (e *jsonRPCError) looksOutOfRange() bool {
	msg := strings.ToLower(e.Message)
	return strings.Contains(msg, "must be between") ||
		strings.Contains(msg, "outside of range") ||
		strings.Contains(msg, "start ledger")
}

// call walks the pool starting at the preferred endpoint until one answers.
func (c *Client) call(ctx context.Context, method string, params any, out any) error {
	start := int(c.preferred.Load())
	var lastErr error
	for i := 0; i < len(c.urls); i++ {
		idx := (start + i) % len(c.urls)
		err := c.callOne(ctx, c.urls[idx], method, params, out)
		if err == nil {
			if idx != start {
				c.failovers.Add(1)
				c.preferred.Store(int32(idx))
			}
			return nil
		}
		// A JSON-RPC-level error is an answer, not an outage: the endpoint
		// is healthy and would give the same reply again. Do not fail over.
		var rpcErr *jsonRPCError
		if errors.As(err, &rpcErr) {
			return err
		}
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return fmt.Errorf("all %d rpc endpoints failed, last: %w", len(c.urls), lastErr)
}

func (c *Client) callOne(ctx context.Context, endpoint, method string, params, out any) error {
	attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()

	reqBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", method, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d from %s", resp.StatusCode, method)
	}

	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *jsonRPCError   `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if out != nil {
		if err := json.Unmarshal(envelope.Result, out); err != nil {
			return fmt.Errorf("decode result: %w", err)
		}
	}
	return nil
}
