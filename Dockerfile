# syntax=docker/dockerfile:1

FROM rust:1.86.0-slim AS builder
WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates musl-tools \
  && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests

RUN cargo build --locked --release --package yarli --bin yarli --target x86_64-unknown-linux-musl
RUN strip target/x86_64-unknown-linux-musl/release/yarli

FROM alpine:3.21 AS runtime
RUN addgroup -S yarli \
  && adduser -S -G yarli yarli \
  && apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/yarli /usr/local/bin/yarli
COPY docker/healthcheck.sh /usr/local/bin/yarli-healthcheck
RUN chmod +x /usr/local/bin/yarli /usr/local/bin/yarli-healthcheck

USER yarli
ENTRYPOINT ["/usr/local/bin/yarli"]
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/yarli-healthcheck"]
