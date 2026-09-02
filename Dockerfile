# syntax=docker/dockerfile:1.7

FROM rust:1.92-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2 AS engine

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake libclang-dev liblz4-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=highwater-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=highwater-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=highwater-cargo-target,target=/build/target \
    cargo build --release --locked --package highwater-server \
    && install -Dm755 target/release/highwater-server /out/highwater-server

FROM python:3.12-slim-bookworm@sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254

LABEL org.opencontainers.image.source="https://github.com/henneberger/highwater" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 highwater \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin highwater \
    && install -d -o 65532 -g 65532 /var/lib/highwater/state /var/lib/highwater/objects
WORKDIR /app
COPY pyproject.toml README.md setup.py ./
COPY src ./src
COPY --from=engine /out/highwater-server /app/src/highwater/bin/highwater-server
RUN python -m pip install --no-cache-dir . \
    && rm -rf /root/.cache /app/src /app/pyproject.toml /app/README.md /app/setup.py
COPY --from=engine /out/highwater-server /usr/local/bin/highwater-server

USER 65532:65532
EXPOSE 7233 7234
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "from urllib.request import urlopen; assert urlopen('http://127.0.0.1:7233/health', timeout=2).status == 200"]
ENTRYPOINT ["highwater-server"]
CMD ["--listen", "0.0.0.0:7233", "--state-dir", "/var/lib/highwater/state", "--object-store-dir", "/var/lib/highwater/objects"]
