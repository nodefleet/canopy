FROM golang:1.23.9-alpine AS builder

RUN apk add --no-cache make bash nodejs npm

ARG EXPLORER_BASE_PATH
ARG WALLET_BASE_PATH

WORKDIR /go/src/github.com/canopy-network/canopy

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy package.json files for npm dependencies
COPY cmd/rpc/web/wallet/package*.json ./cmd/rpc/web/wallet/
COPY cmd/rpc/web/explorer/package*.json ./cmd/rpc/web/explorer/

# Install npm dependencies with cache mount
RUN --mount=type=cache,target=/root/.npm \
    npm ci --prefix ./cmd/rpc/web/wallet --only=production && \
    npm ci --prefix ./cmd/rpc/web/explorer --only=production

# Copy source code (minimize layers)
COPY cmd/ ./cmd/
COPY Makefile ./

ENV EXPLORER_BASE_PATH=${EXPLORER_BASE_PATH}
ENV WALLET_BASE_PATH=${WALLET_BASE_PATH}
ENV CGO_ENABLED=0
ENV GOOS=linux

# Build everything in one layer with cache mounts
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/root/.npm \
    make -j$(nproc) build/wallet build/explorer && \
    go build -ldflags="-s -w" -trimpath -o bin ./cmd/main/...

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /go/src/github.com/canopy-network/canopy/bin ./
ENTRYPOINT ["/app/bin"]
