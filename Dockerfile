# Build a static sierpe binary (no CGO — CLAUDE.md rule 4) and ship it on a
# distroless base: the image is the appliance, nothing else.

# The toolchain tag floats on purpose, unlike the captive core in
# Dockerfile.full. go.mod sets the floor (`toolchain go1.25.13`), so a build
# is never older than what the vulnerability fixes required, and letting the
# tag pick up patch releases is how Go security fixes reach the image without
# a release of ours — CI runs govulncheck unpinned and fails on a stale
# toolchain. Core is pinned because it decides the bytes of replayed history;
# the compiler decides nothing a user can observe. Do not "unify" these.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/sierpe ./cmd/sierpe

FROM gcr.io/distroless/static-debian12:nonroot
# The source label is what links the published package to this repository:
# without it GHCR keeps the package detached, so it never appears on the
# repo page and never inherits its visibility. Registries read it off the
# pushed image, so it has to live in the final stage.
LABEL org.opencontainers.image.source="https://github.com/zkCaleb-dev/sierpe" \
      org.opencontainers.image.description="Self-hosted Stellar contract indexer" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/sierpe /sierpe
EXPOSE 8080
# The probe is the binary itself: distroless has no shell or curl, and the
# platforms that run health checks inside the container (Docker, Swarm,
# Coolify, Dokploy, CapRover, NAS UIs) would otherwise mark it unhealthy
# forever. Kubernetes and cloud load balancers ignore this and probe /health
# over the network, which is also fine.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/sierpe", "healthcheck"]
ENTRYPOINT ["/sierpe"]
CMD ["run"]
