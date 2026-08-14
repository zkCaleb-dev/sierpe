// Command sierpe is the self-hosted Stellar contract indexer.
//
// Subcommands (see docs/DESIGN.md §3):
//
//	run       start the full appliance: ingestion + API (default)
//	serve     start the API only, without the ingestion engine
//	replay    re-ingest a ledger range beside the live process
//	rederive  rebuild derived tables from stored raw data
//	reseed    rebuild the contract watchlist from the database
//	version   print build information
package main

import (
	"fmt"
	"os"
)

// version is stamped at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cmd := "run"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "version":
		fmt.Printf("sierpe %s\n", version)
	case "run", "serve", "replay", "rederive", "reseed":
		fmt.Fprintf(os.Stderr, "sierpe %s: %q is not implemented yet (milestone M0 in progress, see docs/DESIGN.md)\n", version, cmd)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "sierpe: unknown command %q\nusage: sierpe [run|serve|replay|rederive|reseed|version]\n", cmd)
		os.Exit(2)
	}
}
