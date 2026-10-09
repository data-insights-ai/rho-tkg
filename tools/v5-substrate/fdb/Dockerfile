# Optional native arm64 test support. No root/v5 module dependency on this tool.
FROM foundationdb/foundationdb@sha256:c61744624e479d370a0120176e518bb569a2a18ee9abceaf7022c4367b54f1db AS native
FROM golang@sha256:0275e2ab63da9ae2f4f714b2c5a20544b2470d1867919633ebb8d4a8a3d8e6e0
COPY --from=native /usr/lib/libfdb_c.so /usr/local/lib/libfdb_c.so
COPY --from=native /usr/bin/fdbcli /usr/local/bin/fdbcli
RUN curl -fsSL https://github.com/apple/foundationdb/releases/download/7.3.77/fdb-headers-7.3.77.tar.gz -o /tmp/headers.tgz \
 && echo '73b1efca62bf1779e0d4d62131514c96f689db82865695618086e73a057ea385  /tmp/headers.tgz' | sha256sum -c - \
 && mkdir -p /usr/local/include/foundationdb \
 && tar -xzf /tmp/headers.tgz -C /usr/local/include/foundationdb \
 && curl -fsSL https://raw.githubusercontent.com/apple/foundationdb/3ea44ce1d9003ad095e408039e1f755c319c4dfb/bindings/c/foundationdb/fdb_c_types.h -o /usr/local/include/foundationdb/fdb_c_types.h \
 && echo '82ea67e61592873c637e228764f5921179a710826be558ee0d1703023913e14d  /usr/local/include/foundationdb/fdb_c_types.h' | sha256sum -c - \
 && rm /tmp/headers.tgz && ldconfig
WORKDIR /harness
ENV GOTOOLCHAIN=local GOWORK=off
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -o /usr/local/bin/fdb-spike .
CMD ["fdb-spike", "smoke"]
