FROM rust:1.92-bookworm AS engine

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang cmake libclang-dev liblz4-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked --package highwater-server

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
ENV PYTHONPATH=/app
COPY pyproject.toml README.md ./
COPY src ./src
COPY examples ./examples
RUN python -m pip install --no-cache-dir .
COPY --from=engine /build/target/release/highwater-server /usr/local/bin/highwater-server
COPY deploy/fly/entrypoint.sh /usr/local/bin/highwater-cloud
RUN chmod 0755 /usr/local/bin/highwater-cloud

EXPOSE 8080
CMD ["/usr/local/bin/highwater-cloud", "server"]
