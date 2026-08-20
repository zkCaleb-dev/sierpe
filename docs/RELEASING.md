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

Build and push the images (until CI does it). The slim image MUST be
built with buildx as multi-platform: a plain `docker build` produces an
image for the build host's architecture only, and an arm64-only image
fails to pull on every amd64 platform — this bricked the first Railway
deployment (found live 2026-08-20). The `-full` variant stays amd64-only
because SDF ships stellar-core for amd64 exclusively.

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/zkcaleb-dev/sierpe:vX.Y.Z --build-arg VERSION=vX.Y.Z --push .
docker build -f Dockerfile.full --platform linux/amd64 \
  -t ghcr.io/zkcaleb-dev/sierpe:vX.Y.Z-full --build-arg VERSION=vX.Y.Z .
docker push ghcr.io/zkcaleb-dev/sierpe:vX.Y.Z-full
```

Verify both architectures landed before announcing anything:

```bash
docker manifest inspect ghcr.io/zkcaleb-dev/sierpe:vX.Y.Z | grep architecture
```

## Verify before publishing the draft

- `docker run` the pushed image against a scratch Postgres and testnet:
  boot, register a contract, watch `/status` reach ready.
- `sierpe version` on one downloaded binary prints the tag.

Then publish the draft release and update the Railway template to the new
image tag.
