# Multi-stage production build for Roviers
FROM rust:1.82-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p roviers-server -p roviers-cli

# Minimal Debian runtime container
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 10001 roviers && useradd -u 10001 -g roviers -s /bin/false -m roviers

WORKDIR /home/roviers
COPY --from=builder /app/target/release/roviers-server /usr/local/bin/roviers-server
COPY --from=builder /app/target/release/roviers-cli /usr/local/bin/roviers-cli

RUN mkdir -p /var/lib/roviers && chown -R roviers:roviers /var/lib/roviers

USER roviers
EXPOSE 8080

VOLUME ["/var/lib/roviers"]
HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/roviers-server"]
CMD ["--bind", "0.0.0.0:8080", "--data-dir", "/var/lib/roviers"]

