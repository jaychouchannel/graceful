# Single-stage build using Alpine's packaged Go toolchain.
# Chosen so the image can be built without pulling the much larger
# golang:*-alpine base image (network-restricted environments still work).
FROM alpine:latest

# Install Go toolchain, git not needed (no external deps), and runtime certs.
RUN apk add --no-cache go ca-certificates

WORKDIR /app
COPY go.mod ./
COPY . .

# Static binary so no glibc/musl runtime dependency in the final layer.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags='-s -w' -o /graceful ./cmd/graceful \
    && rm -rf /app

ENTRYPOINT ["/graceful"]
EXPOSE 8080