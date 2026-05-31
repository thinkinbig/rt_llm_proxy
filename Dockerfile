FROM golang:1.25-bookworm AS build

# Docker: use goproxy.io (proxy.golang.org often TLS-times out in containers; Go does
# not fall through to the next entry on proxy network errors).
# China: docker compose -f docker-compose.yml -f docker-compose.cn.yml build
# Host dev abroad: go env -w GOPROXY=https://proxy.golang.org,direct
ARG GOPROXY=https://goproxy.io,direct
ENV GOPROXY=${GOPROXY}

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus-dev libopusfile-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /proxy ./cmd/proxy

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopus0 libopusfile0 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /proxy /usr/local/bin/proxy
COPY demo /demo

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/proxy"]
