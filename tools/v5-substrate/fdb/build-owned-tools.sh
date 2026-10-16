#!/bin/bash
# Invoked only in the existing capped pinned-Go prebuild container. No host install.
set -euo pipefail
[ "$(. /etc/os-release; printf '%s/%s' "$ID" "$VERSION_CODENAME")" = debian/trixie ]
source_sha=bbd141ef7b5d0127cc2152843ba61f274dc32814fa3e0f13e7d07a080bef53d9
mkdir -p /owned-tools /tmp/rho-iproute-build
cd /tmp/rho-iproute-build
# An immutable package-index date; record actual package versions and hashes.
# No latest repository fallback. Existing compiler/libc originate in the pinned image.
rm -f /etc/apt/sources.list /etc/apt/sources.list.d/*
printf '%s\n' 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261001T000000Z/ trixie main' > /etc/apt/sources.list
apt-get -o Acquire::Retries=0 update
apt-get -o Acquire::Retries=0 --no-install-recommends -y install bison flex xz-utils
curl --fail --location --max-time 60 --output source.tar.xz https://mirrors.edge.kernel.org/pub/linux/utils/net/iproute2/iproute2-6.12.0.tar.xz
printf '%s  source.tar.xz\n' "$source_sha" | sha256sum --check
# Fixed archive hash authenticates names/content; no caller archive is extracted.
tar --extract --xz --file source.tar.xz
cd iproute2-6.12.0
PKG_CONFIG=/bin/false sh configure --libbpf_force off
make -j2 SUBDIRS='lib ip tc' SHARED_LIBS=n LDFLAGS=-static
install -m755 tc/tc ip/ip /owned-tools/
# No interpreter segment: the tools must not load undeclared container libraries.
readelf -h -l /owned-tools/tc /owned-tools/ip > /owned-tools/elf.txt
if grep -q INTERP /owned-tools/elf.txt; then exit 1; fi
{ printf 'SourceSHA256=%s\nSnapshot=20261001T000000Z\nStaticELF=no-INTERP\n' "$source_sha"; gcc --version; make --version; bison --version; flex --version; dpkg-query -W; sha256sum /usr/bin/gcc /usr/bin/bison /usr/bin/flex; cat /owned-tools/elf.txt; } > /owned-tools/build.txt
cd /harness
CGO_ENABLED=0 go build -trimpath -o /usr/local/bin/transport-probe ./transportprobe
printf '{"Version":"iproute2-6.12.0","TC":"%s","IP":"%s","SourceSHA256":"%s","BuildSHA256":"%s"}\n' "$(sha256sum /owned-tools/tc | cut -d' ' -f1)" "$(sha256sum /owned-tools/ip | cut -d' ' -f1)" "$source_sha" "$(sha256sum /owned-tools/build.txt | cut -d' ' -f1)" > /owned-tools/manifest.json
