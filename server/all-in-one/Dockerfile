# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26.8-alpine3.23 AS build

RUN apk add --no-cache ca-certificates
WORKDIR /src
COPY . .
WORKDIR /src/server/all-in-one
RUN GOWORK=off go mod download

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 GOWORK=off GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${BUILD_DATE}" \
    -o /out/dex-server-all-in-one ./cmd/dex-server-all-in-one && \
    CGO_ENABLED=0 GOWORK=off GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" \
    -o /out/temporal-sql-tool ./cmd/temporal-sql-tool

FROM alpine:3.23

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S -g 10001 dex && \
    adduser -S -D -H -u 10001 -G dex dex

COPY --from=build /out/dex-server-all-in-one /usr/local/bin/dex-server-all-in-one
COPY --from=build /out/temporal-sql-tool /usr/local/bin/temporal-sql-tool

USER 10001:10001

EXPOSE 8801 7233 7234 7235 7239 6933 6934 6935 6939
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=6 \
    CMD ["/usr/local/bin/dex-server-all-in-one", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/dex-server-all-in-one"]
CMD ["serve"]
