FROM golang:1.27.1-alpine AS build

ARG TARGET
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN test -n "$TARGET" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service "./cmd/$TARGET"

FROM alpine:3.22

RUN addgroup -S queue && adduser -S -G queue queue
COPY --from=build /out/service /usr/local/bin/service

USER queue
ENTRYPOINT ["/usr/local/bin/service"]
