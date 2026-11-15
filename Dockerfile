# Build stage: compile a static binary.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /scheduler ./cmd/scheduler

# Run stage: small image, non-root user. wget (busybox) is used by the healthcheck.
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /scheduler /scheduler
EXPOSE 8080 6060
HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://localhost:8080/readyz || exit 1
ENTRYPOINT ["/scheduler"]
