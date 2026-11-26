FROM golang:1.24.8-bookworm AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN curl -fsSL --retry 2 \
    https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-10-15T17-29-55Z \
    -o minio.tar.gz \
 && echo "be6d0bd3696c3a13a35f02d3a0280b64319c67918b4501c5c3d87f96d000085c  minio.tar.gz" | sha256sum -c - \
 && tar -xzf minio.tar.gz --strip-components=1 \
 && go build -trimpath \
    -ldflags '-s -w -X github.com/minio/minio/cmd.Version=2025-10-15T17:29:55Z -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-10-15T17-29-55Z' \
    -o /minio .

FROM alpine:3.22.1
RUN apk add --no-cache ca-certificates curl
COPY --from=build /minio /usr/local/bin/minio
ENTRYPOINT ["minio"]
