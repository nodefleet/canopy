# Build cache for Go modules
FROM golang:1.23.9-alpine AS go-deps
WORKDIR /go/src/github.com/canopy-network/canopy
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build cache for npm dependencies  
FROM node:18-alpine AS npm-deps
WORKDIR /app
COPY cmd/rpc/web/wallet/package*.json ./wallet/
COPY cmd/rpc/web/explorer/package*.json ./explorer/
RUN --mount=type=cache,target=/root/.npm \
    cd wallet && npm ci --only=production && \
    cd ../explorer && npm ci --only=production

# Main builder stage
FROM golang:1.23.9-alpine AS builder
RUN apk add --no-cache make bash

ARG EXPLORER_BASE_PATH
ARG WALLET_BASE_PATH

WORKDIR /go/src/github.com/canopy-network/canopy

# Copy dependencies from cache stages
COPY --from=go-deps /go/pkg/mod /go/pkg/mod
COPY --from=npm-deps /app/wallet/node_modules ./cmd/rpc/web/wallet/node_modules
COPY --from=npm-deps /app/explorer/node_modules ./cmd/rpc/web/explorer/node_modules

# Copy source files
COPY go.mod go.sum ./
COPY cmd/ ./cmd/
COPY Makefile ./

ENV EXPLORER_BASE_PATH=${EXPLORER_BASE_PATH}
ENV WALLET_BASE_PATH=${WALLET_BASE_PATH}
ENV CGO_ENABLED=0
ENV GOOS=linux

# Build in parallel using make's job control
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make -j$(nproc) build/wallet build/explorer && \
    go build -ldflags="-s -w" -trimpath -o bin ./cmd/main/...

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /go/src/github.com/canopy-network/canopy/bin ./
ENTRYPOINT ["/app/bin"]
