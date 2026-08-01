# Build stage
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags='-s -w' -o /graceful ./cmd/graceful

# Final stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates
COPY --from=builder /graceful /usr/local/bin/graceful
ENTRYPOINT ["graceful"]
EXPOSE 8080