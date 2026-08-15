# Build a static sierpe binary (no CGO — CLAUDE.md rule 4) and ship it on a
# distroless base: the image is the appliance, nothing else.

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
COPY --from=build /out/sierpe /sierpe
EXPOSE 8080
ENTRYPOINT ["/sierpe"]
CMD ["run"]
