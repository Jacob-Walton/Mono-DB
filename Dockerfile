FROM rustlang/rust:nightly-slim AS builder

WORKDIR /build

# System deps
RUN apt-get update && apt-get install -y \
    pkg-config \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build real code
COPY . .
RUN cargo +nightly build --release

# Runtime stage
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /var/lib/monodb

# Binaries
COPY --from=builder /build/target/release/monod ./bin/monod
COPY --from=builder /build/target/release/mdb /usr/local/bin/mdb
COPY --from=builder /build/target/release/mdb_ready /usr/local/bin/mdb_ready

# Default config
COPY config.toml ./config.toml

EXPOSE 6432

VOLUME ["/var/lib/monodb/data"]

STOPSIGNAL SIGTERM

ENTRYPOINT ["/var/lib/monodb/bin/monod"]
CMD ["--config", "/var/lib/monodb/config.toml", "--data", "/var/lib/monodb/data"]
