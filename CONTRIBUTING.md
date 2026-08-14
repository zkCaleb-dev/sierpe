# Contributing to Sierpe

Thanks for your interest. Sierpe is early — the highest-value contributions
right now are design review, testing against real contracts, and milestone
work from [docs/DESIGN.md](docs/DESIGN.md) §13.

## Ground rules

- Read `docs/DESIGN.md` first. Design decisions D1–D8 are recorded there;
  if you disagree with one, open a Discussion — don't ship a PR that
  re-litigates it silently.
- The architecture in `CLAUDE.md` is binding: package boundaries, the
  atomic cursor+data commit, no CGO, small consumer-side interfaces.
- Keep PRs focused: one concern per PR.

## Building and testing

```bash
go build ./...
go vet ./...
go test -race ./...
```

CI additionally runs gofmt, staticcheck, gosec, and govulncheck — run them
locally if you can. A PR that fails CI will not be reviewed.

## Commit style

- [Conventional Commits](https://www.conventionalcommits.org/):
  `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`, imperative mood.
- No AI co-authorship trailers.

## What needs tests

Everything, but especially the distrust paths: failed-transaction filtering,
XDR panic frontiers, removal plausibility guards, gap persistence, and
resume-with-hash-check. A change to any of these without a test will be
asked to add one.

## Reporting bugs

Open an issue with: what you expected, what happened, your deployment shape
(Railway / compose / other), `NETWORK`, and the relevant `/v1/status` output
if you can share it. Never paste your `ADMIN_TOKEN` or `DATABASE_URL`.

## Security issues

Do **not** open a public issue — see [SECURITY.md](SECURITY.md).
