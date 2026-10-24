# syntax=docker/dockerfile:1.7

FROM rust:1.97.1-bookworm AS builder

ARG SERVER_ZD_VERSION=
ENV CARGO_BUILD_JOBS=4
WORKDIR /src

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        clang cmake libclang-dev libssl-dev pkg-config python3 zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY LICENSE ./LICENSE
COPY protos/ ./protos/
COPY s3mock/ ./s3mock/
COPY server-zd/ ./server-zd/

WORKDIR /src/server-zd
RUN python3 - <<'PY'
import os
import re
import tomllib
from pathlib import Path

manifest_path = Path('Cargo.toml')
lock_path = Path('Cargo.lock')
manifest = tomllib.loads(manifest_path.read_text())
original_version = manifest['workspace']['package']['version']
release_version = os.environ['SERVER_ZD_VERSION']
if not release_version:
    raise SystemExit(0)
members = manifest['workspace']['members']
package_names = {
    tomllib.loads((Path(member) / 'Cargo.toml').read_text())['package']['name']
    for member in members
}
manifest_text = manifest_path.read_text()
manifest_text, replacements = re.subn(
    rf'(?m)^(version = "){re.escape(original_version)}("$)',
    rf'\g<1>{release_version}\g<2>',
    manifest_text,
    count=1,
)
if replacements != 1:
    raise SystemExit('failed to stamp workspace version')
manifest_path.write_text(manifest_text)
lock_text = lock_path.read_text()
for package_name in package_names:
    lock_text, replacements = re.subn(
        rf'(?m)(^name = "{re.escape(package_name)}"\nversion = "){re.escape(original_version)}("$)',
        rf'\g<1>{release_version}\g<2>',
        lock_text,
        count=1,
    )
    if replacements != 1:
        raise SystemExit(f'failed to stamp {package_name} lock version')
lock_path.write_text(lock_text)
PY
ARG TARGETARCH
RUN --mount=type=cache,id=dex-server-zd-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=dex-server-zd-target-${TARGETARCH},target=/src/server-zd/target \
    cargo build --release --locked -p dexzd-server \
    && mkdir -p /out \
    && cp target/release/dex-server-zd /out/dex-server-zd

FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates libgcc-s1 libstdc++6 \
    && groupadd --system --gid 10001 dex \
    && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin dex \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/dex-server-zd /usr/local/bin/dex-server-zd
COPY server-zd/config/dex-server-zd.example.yaml /etc/dex-server-zd/dex-server-zd.example.yaml
COPY server/LICENSE.md server/LEGACY_NOTICES.md /licenses/
COPY server/LICENSES/ /licenses/LICENSES/

USER 10001:10001
EXPOSE 8801
ENTRYPOINT ["/usr/local/bin/dex-server-zd"]
CMD ["serve", "--config", "/etc/dex-server-zd/config.yaml"]
