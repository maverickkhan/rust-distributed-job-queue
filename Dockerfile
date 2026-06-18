# syntax=docker/dockerfile:1

# ---- Builder ----------------------------------------------------------------
# buildpack-deps base provides gcc/make; cmake + clang cover the rustls crypto
# backend's native build.
FROM rust:1-bookworm AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*

# Copy the whole workspace and build the two release binaries.
COPY . .
RUN cargo build --release -p djq-api -p djq-worker

# ---- Runtime ----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m app

COPY --from=builder /app/target/release/djq-api /usr/local/bin/djq-api
COPY --from=builder /app/target/release/djq-worker /usr/local/bin/djq-worker

USER app
ENV API_BIND_ADDR=0.0.0.0:8080
EXPOSE 8080 9091

# Default to the API; the worker service overrides this in docker-compose.
CMD ["djq-api"]
