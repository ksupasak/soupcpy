# syntax=docker/dockerfile:1

# ---- build -----------------------------------------------------------------
FROM rust:1-slim-bookworm AS builder

WORKDIR /build

# Compile the dependency graph first so edits under src/ don't invalidate it.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src

# templates/ is pulled in with include_str!, so it must exist at compile time.
COPY src ./src
COPY templates ./templates
RUN touch src/main.rs && cargo build --release

# ---- runtime ---------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 soupcpy

WORKDIR /app

# ServeDir::new("public") and the default config path both resolve against the
# working directory, so these have to sit next to each other under /app.
COPY public ./public
COPY config ./config

# The crate is still named "rust" in Cargo.toml, hence the binary name here.
COPY --from=builder /build/target/release/rust /usr/local/bin/soupcpy-gateway

USER soupcpy

ENV RUST_LOG=info
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:3000/health || exit 1

CMD ["soupcpy-gateway"]
