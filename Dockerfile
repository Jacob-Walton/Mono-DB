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
FROM debian:bookworm-slim

WORKDIR /opt/monodb

# Binaries
COPY --from=builder /build/target/release/monod ./bin/monod
COPY --from=builder /build/target/release/mdb /usr/local/bin/mdb
COPY --from=builder /build/target/release/mdb_ready /usr/local/bin/mdb_ready

# Default config
COPY config.toml ./config.toml

EXPOSE 7899

VOLUME ["/opt/monodb/data"]

ENTRYPOINT ["/opt/monodb/bin/monod"]
CMD ["--config", "/opt/monodb/config.toml", "--data", "/opt/monodb/data"]