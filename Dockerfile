# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gigamq ./cmd/server/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /app/gigamq /gigamq

EXPOSE 9000

ENTRYPOINT ["/gigamq"]
