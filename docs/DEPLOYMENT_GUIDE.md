# Roviers Server Deployment & Operations Guide 🏛️

This guide covers production deployment, configuration, operating system tuning, and containerization for **Roviers** servers.

---

## 📋 Table of Contents
1. [Architecture & Hardware Sizing](#1-architecture--hardware-sizing)
2. [Building and Installing](#2-building-and-installing)
3. [Configuration Options](#3-configuration-options)
4. [Container Deployment (Docker & Compose)](#4-container-deployment-docker--compose)
5. [Bare-Metal & VM Deployment (Systemd)](#5-bare-metal--vm-deployment-systemd)
6. [Reverse Proxy Configuration (Nginx & Caddy)](#6-reverse-proxy-configuration-nginx--caddy)
7. [Kernel & OS Tuning](#7-kernel--os-tuning)
8. [Storage, Retention & Backup Strategy](#8-storage-retention--backup-strategy)
9. [Observability & Health Checks](#9-observability--health-checks)

---

## 1. Architecture & Hardware Sizing

Roviers uses a high-performance **memory-mapped append-only ledger (`memmap2`)** coupled with zero-copy cryptographic verification. Because it does not run heavy transactional SQL locks or maintain synchronous connection pools, resource requirements are predictable and lightweight.

### Recommended Profiles

| Profile | Target Load | vCPU | RAM | Storage | Typical Cloud Instance |
|:---|:---|:---|:---|:---|:---|
| **Starter / Edge** | Up to 15,000 req/s | 2 cores | 4 GB | 50 GB NVMe / SSD | AWS `t4g.medium` / GCP `e2-standard-2` |
| **Standard Production** | 50,000 – 120,000 req/s | 4–8 cores | 16 GB | 250 GB NVMe | AWS `c6i.xlarge` / GCP `c3-standard-4` |
| **High-Throughput Node** | 200,000+ req/s | 16 cores | 32 GB | 1 TB Enterprise NVMe | AWS `c6i.4xlarge` / GCP `c3-standard-16` |

> [!NOTE]
> **Disk I/O Type:** Sequential write speed matters most. Standard NVMe or fast block storage (e.g., AWS EBS `gp3` with 3,000+ IOPS or GCP Hyperdisk Balanced) provides optimal throughput.

---

## 2. Building and Installing

### Prerequisites
- **Rust Toolchain:** Rust 1.80+ (or edition 2024 compatible)
- **C Compiler:** `clang` or `gcc` (for standard dependency builds)

### Production Build
Clone and compile with full compiler optimizations and CPU feature acceleration:

```bash
git clone https://github.com/sunilksundaram/roviers.git
cd roviers

# Build release binaries with native CPU target optimizations
RUSTFLAGS="-C target-cpu=native" cargo build --release -p roviers-server -p roviers-cli
```

Binaries will be placed in `target/release/`:
- `roviers-server`: Core HTTP/SSE service counter, ledger engine, and outcome vault.
- `roviers-cli`: Administration, client credential issuance, and verification tooling.

Install to system path:
```bash
sudo install -m 755 target/release/roviers-server /usr/local/bin/
sudo install -m 755 target/release/roviers-cli /usr/local/bin/
```

---

## 3. Configuration Options

`roviers-server` is configured via command-line flags and environment variables:

| Argument | Environment Variable | Default | Description |
|:---|:---|:---|:---|
| `-b`, `--bind` | `ROVIERS_BIND` | `127.0.0.1:8080` | IP and TCP port to bind HTTP/SSE server |
| `-d`, `--data-dir` | `ROVIERS_DATA_DIR` | `./roviers_data` | Directory path where ledger segments and vault data reside |

To run on all network interfaces in production:
```bash
roviers-server --bind 0.0.0.0:8080 --data-dir /var/lib/roviers
```

---

## 4. Container Deployment (Docker & Compose)

### Production Multi-Stage Dockerfile

Create `Dockerfile`:
```dockerfile
# Stage 1: Build
FROM rust:bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p roviers-server

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
RUN groupadd -g 10001 roviers && useradd -u 10001 -g roviers -s /bin/false -m roviers

WORKDIR /home/roviers
COPY --from=builder /app/target/release/roviers-server /usr/local/bin/roviers-server

RUN mkdir -p /var/lib/roviers && chown -R roviers:roviers /var/lib/roviers

USER roviers
EXPOSE 8080

VOLUME ["/var/lib/roviers"]
ENTRYPOINT ["/usr/local/bin/roviers-server"]
CMD ["--bind", "0.0.0.0:8080", "--data-dir", "/var/lib/roviers"]
```

### Docker Compose (`docker-compose.yml`)

```yaml
version: '3.8'

services:
  roviers:
    image: roviers:latest
    build:
      context: .
      dockerfile: Dockerfile
    container_name: roviers-service-counter
    restart: unless-stopped
    ports:
      - "8080:8080"
    environment:
      - RUST_LOG=info,roviers_core=info,roviers_server=info
    volumes:
      - roviers_storage:/var/lib/roviers
    ulimits:
      nofile:
        soft: 65536
        hard: 65536

volumes:
  roviers_storage:
    driver: local
```

Launch with:
```bash
docker compose up -d
```

---

## 5. Bare-Metal & VM Deployment (Systemd)

For running directly on Linux VMs (Ubuntu, Debian, RHEL, Rocky):

### 1. Create Dedicated Service User & Storage Directory
```bash
sudo useradd -r -s /bin/false roviers
sudo mkdir -p /var/lib/roviers
sudo chown -R roviers:roviers /var/lib/roviers
sudo chmod 750 /var/lib/roviers
```

### 2. Create Systemd Unit (`/etc/systemd/system/roviers.service`)
```ini
[Unit]
Description=Roviers Task Broker & Claim-Check Vault
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=roviers
Group=roviers
ExecStart=/usr/local/bin/roviers-server --bind 127.0.0.1:8080 --data-dir /var/lib/roviers
Restart=always
RestartSec=3s

# Security hardening
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true

# High concurrency file descriptor limits
LimitNOFILE=1048576
LimitNPROC=512000

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=roviers
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

### 3. Enable and Start
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now roviers
sudo systemctl status roviers
```

---

## 6. Reverse Proxy Configuration (Nginx & Caddy)

Because Roviers serves both standard HTTP requests and long-lived **Server-Sent Events (SSE)** for zero-polling intimations, your reverse proxy must disable response buffering.

### Nginx Configuration (`/etc/nginx/sites-available/roviers.conf`)

```nginx
upstream roviers_backend {
    server 127.0.0.1:8080;
    keepalive 256;
}

server {
    listen 80;
    server_name roviers.internal.example.com;

    # Client payload size (match maximum expected request envelope)
    client_max_body_size 64M;

    # Standard API endpoints
    location / {
        proxy_pass http://roviers_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # CRITICAL: SSE Signal Stream configuration
    location ~* /api/v1/agents/.+/stream {
        proxy_pass http://roviers_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;

        # Disable buffering so signals stream immediately to agents
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding off;

        # Keep connection open without proxy timeouts
        proxy_read_timeout 24h;
        proxy_send_timeout 24h;
    }
}
```

### Caddy Configuration (`Caddyfile`)

```caddy
roviers.internal.example.com {
    reverse_proxy 127.0.0.1:8080 {
        # Streaming friendly defaults
        flush_interval -1
    }
}
```

---

## 7. Kernel & OS Tuning

For sustained throughput above 50,000 requests/sec, tune the following Linux kernel parameters in `/etc/sysctl.d/99-roviers.conf`:

```ini
# Increase socket backlog and connection table size
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535

# Fast TCP reuse for high-frequency client churn
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15

# Memory-mapped file limits (critical for memmap2 segments)
vm.max_map_count = 1048576

# Flush dirty pages smoothly without IO stalls
vm.dirty_background_ratio = 5
vm.dirty_ratio = 10

# Increase local port range for outbound proxies
net.ipv4.ip_local_port_range = 1024 65535
```

Apply immediately:
```bash
sudo sysctl --system
```

Also verify file descriptor limits in `/etc/security/limits.d/99-roviers.conf`:
```ini
roviers soft nofile 1048576
roviers hard nofile 1048576
```

---

## 8. Storage, Retention & Backup Strategy

Roviers stores data in an append-only directory layout:
```
roviers_data/
├── segment_0000000000.log      # Active or closed mmap log segments (64MB chunks)
├── segment_0000000001.log
└── ...
```

### Hot vs. Cold Segment Archival
1. **Active Segments:** Roviers writes sequentially to the tail segment.
2. **Completed Segments:** Past segments are immutable and closed. They can be safely backed up or replicated using standard tools without locking or shutting down the service:
   ```bash
   # Synchronize closed segments to S3 / Cloud Storage every hour
   aws s3 sync /var/lib/roviers/ s3://my-backup-bucket/roviers-ledger/ --exclude "*.tmp"
   ```

### Disaster Recovery
To restore a node, restore the log segment files into the designated data directory and launch `roviers-server`. The engine re-opens the segments, verifies CRC32 checksums, and restores state on startup.

---

## 9. Observability & Health Checks

### Liveness & Readiness Probes
- **Endpoint:** `GET /health`
- **Expected Return:** HTTP `200 OK` with payload `"Roviers Broker: OK"`

Kubernetes Pod spec snippet:
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 2
  periodSeconds: 5
readinessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 2
  periodSeconds: 3
```

### Metrics & Tracing
Roviers emits structured JSON logs via `tracing-subscriber`. To ship logs to Datadog, Grafana Loki, or Google Cloud Logging:
```bash
RUST_LOG=info,roviers_core=debug roviers-server --bind 0.0.0.0:8080
```

