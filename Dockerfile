# pogi: AWS, Google Cloud and Azure emulators in one process.
#   docker run --rm -p 127.0.0.1:7644:7644 -p 127.0.0.1:10000-10002:10000-10002 ghcr.io/nrndev/pogi
#
# The image is a minimal Alpine with Python and Node.js, so Lambda, Cloud Functions and Azure
# Functions run natively in the container and init scripts (.sh, .py) run. Add the AWS SDKs functions
# expect with `pogi lambda-sdks` (docker exec). Mounting /var/run/docker.sock is optional: then
# provided.* runtimes and container-backed services (RDS, ECS, EKS…) work too, but that socket gives
# pogi, and anyone who can reach its port, control of your Docker.
# Keep this tag equal to the go line in go.mod. The image sets GOTOOLCHAIN=local, so an older cached
# 1.27 image cannot fetch the Go that go.mod asks for, and the build fails.
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X github.com/nrndev/pogi.Version=${VERSION}" -o /pogi ./cmd/pogi

# The image: only what functions and init scripts need.
FROM alpine:3.22
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates python3 nodejs
COPY --from=build /pogi /usr/local/bin/pogi
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
ENV POGI_HOST=0.0.0.0 POGI_ALLOW_EXTERNAL=1 POGI_DATA_DIR=/var/lib/pogi
VOLUME /var/lib/pogi
EXPOSE 7644 4510 1883 2222 2525 5672 10000 10001 10002
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
  CMD ["pogi", "wait", "--timeout", "2s", "--endpoint", "http://127.0.0.1:7644"]
ENTRYPOINT ["docker-entrypoint.sh"]
