FROM golang:1.27.1-alpine AS build

WORKDIR /src
COPY . .
WORKDIR /src/tools/consensus-proof
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/queue-node ./cmd/queue-node

FROM alpine:3.22

RUN addgroup -S queue && adduser -S -G queue queue && \
    mkdir -p /var/lib/distributed-queue && \
    chown queue:queue /var/lib/distributed-queue
COPY --from=build /out/queue-node /usr/local/bin/queue-node

USER queue
ENTRYPOINT ["/usr/local/bin/queue-node"]
