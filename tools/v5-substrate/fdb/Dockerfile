# Optional native arm64 test support. No root/v5 module dependency on this tool.
FROM foundationdb/foundationdb@sha256:c61744624e479d370a0120176e518bb569a2a18ee9abceaf7022c4367b54f1db AS native
FROM golang@sha256:0275e2ab63da9ae2f4f714b2c5a20544b2470d1867919633ebb8d4a8a3d8e6e0 AS source
COPY --from=native /usr/lib/libfdb_c.so /usr/local/lib/libfdb_c.so
COPY --from=native /usr/bin/fdbcli /usr/local/bin/fdbcli
WORKDIR /harness
ENV GOTOOLCHAIN=local GOWORK=off GOMAXPROCS=2 GOMEMLIMIT=768MiB GOFLAGS=-p=1
COPY go.mod go.sum ./
COPY *.go ./
COPY prepare-client.sh build-owned-tools.sh ./
COPY transportprobe/ ./transportprobe/

# The local launcher builds only source, then compiles in a separately capped
# container before starting any server. The default image remains executable.
FROM source AS compiled
RUN bash prepare-client.sh && go mod download
RUN go build -o /usr/local/bin/fdb-spike .
CMD ["fdb-spike", "smoke"]
