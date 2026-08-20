# Releasing

Docs are part of the definition of done (DESIGN.md §12); so is this runbook.

## Preconditions

- `main` green on the full local gate: `gofmt -l .`, `go build ./...`,
  `go vet ./...`, `go test -race ./...` (with `SIERPE_TEST_DATABASE_URL`
  pointing at a throwaway Postgres), staticcheck.
- Live tests passing: `SIERPE_LIVE_TEST=1 go test ./internal/source/rpc -run TestLive -v`.
- CHANGELOG.md updated: move `[Unreleased]` under the new version with the
  date.
- The project name is **sierpe** (final as of v1.0.0). The tag freezes the
  module path, the image name and the API surface in the ecosystem's memory.

## Cut

```bash
git tag -a vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
goreleaser release --clean   # builds static binaries, drafts the GitHub release
```

Build and push the image (until CI does it):

```bash
docker build -t ghcr.io/zkcaleb-dev/sierpe:vX.Y.Z --build-arg VERSION=vX.Y.Z .
docker push ghcr.io/zkcaleb-dev/sierpe:vX.Y.Z
```

## Verify before publishing the draft

- `docker run` the pushed image against a scratch Postgres and testnet:
  boot, register a contract, watch `/status` reach ready.
- `sierpe version` on one downloaded binary prints the tag.

Then publish the draft release and update the Railway template to the new
image tag.
