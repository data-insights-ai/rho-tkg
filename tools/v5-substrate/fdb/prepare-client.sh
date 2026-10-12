#!/bin/bash
set -euo pipefail
curl --max-time 90 -fsSL https://github.com/apple/foundationdb/releases/download/7.3.77/fdb-headers-7.3.77.tar.gz -o /tmp/headers.tgz
echo '73b1efca62bf1779e0d4d62131514c96f689db82865695618086e73a057ea385  /tmp/headers.tgz' | sha256sum -c -
mkdir -p /usr/local/include/foundationdb
tar -xzf /tmp/headers.tgz -C /usr/local/include/foundationdb
curl --max-time 90 -fsSL https://raw.githubusercontent.com/apple/foundationdb/3ea44ce1d9003ad095e408039e1f755c319c4dfb/bindings/c/foundationdb/fdb_c_types.h -o /usr/local/include/foundationdb/fdb_c_types.h
echo '82ea67e61592873c637e228764f5921179a710826be558ee0d1703023913e14d  /usr/local/include/foundationdb/fdb_c_types.h' | sha256sum -c -
rm /tmp/headers.tgz
ldconfig
