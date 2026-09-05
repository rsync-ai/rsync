# Deploy rsync.ai on Oracle Cloud Free Tier

Free forever — 4 OCPU + 24 GB RAM ARM VM. No credit card charges after signup.

---

## Step 1 — Create Oracle Cloud account

1. Go to https://signup.cloud.oracle.com
2. Sign up — you will need a credit/debit card for identity verification only (₹0 charged)
3. Select your home region (Mumbai is closest for India)

---

## Step 2 — Create the VM (Always Free ARM)

1. In the Oracle Console → **Compute → Instances → Create Instance**
2. Change the shape:
   - Click **Change Shape**
   - Select **Ampere** (ARM)
   - Choose **VM.Standard.A1.Flex**
   - Set **4 OCPUs** and **24 GB RAM**
3. OS Image: **Ubuntu 22.04** (recommended)
4. Networking: Create a new VCN, allow public IP
5. SSH Keys: Upload your public key (`~/.ssh/id_rsa.pub`)
6. Click **Create**

> The VM is **Always Free** — it will not expire or be charged.

---

## Step 3 — Open firewall ports

In Oracle Console → **Networking → Virtual Cloud Networks → your VCN → Security Lists → Default**

Add **Ingress Rules**:

| Protocol | Port | Description |
|---|---|---|
| TCP | 22 | SSH |
| TCP | 80 | HTTP (redirects to HTTPS) |
| TCP | 443 | HTTPS |

Also open the OS firewall on the VM itself:
```bash
sudo iptables -I INPUT -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save
```

---

## Step 4 — SSH into the VM

```bash
ssh ubuntu@<your-vm-public-ip>
```

---

## Step 5 — Install Docker

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker compose version
```

---

## Step 6 — Point your domain (optional but recommended)

In your domain registrar (GoDaddy, Namecheap, etc.):
- Add an **A record**: `@` → your VM public IP
- Add an **A record**: `www` → your VM public IP

Wait ~5 min for DNS propagation.

---

## Step 7 — Install rsync.ai

```bash
curl -sSL https://raw.githubusercontent.com/rsync-ai/rsync/main/install.sh | bash
```

The installer will:
- Ask for your OpenAI API key
- Ask for your domain or IP
- Ask for your admin email
- Generate all secrets automatically
- Pull Docker images and start everything

---

## Step 8 — Set up HTTPS with Traefik (if you have a domain)

rsync.ai uses **Traefik v3** as its reverse proxy with automatic Let's Encrypt TLS.

```bash
cd ~/rsync-ai

# Create Traefik config directory
mkdir -p traefik/letsencrypt
touch traefik/letsencrypt/acme.json
chmod 600 traefik/letsencrypt/acme.json

# Write Traefik static config
cat > traefik/traefik.yml << 'EOF'
api:
  dashboard: false

log:
  level: INFO
  format: json

accessLog:
  format: json
  filters:
    statusCodes:
      - "400-599"

entryPoints:
  web:
    address: ":80"
    http:
      redirections:
        entryPoint:
          to: websecure
          scheme: https
          permanent: true
  websecure:
    address: ":443"
    http:
      tls:
        certResolver: letsencrypt

certificatesResolvers:
  letsencrypt:
    acme:
      email: YOUR_ADMIN_EMAIL
      storage: /letsencrypt/acme.json
      httpChallenge:
        entryPoint: web

providers:
  docker:
    exposedByDefault: false
    network: rsync-ai_default
  file:
    filename: /etc/traefik/dynamic.yml
    watch: true
EOF

# Write Traefik dynamic config (security headers + TLS hardening)
cat > traefik/dynamic.yml << 'EOF'
tls:
  options:
    default:
      minVersion: VersionTLS12
      cipherSuites:
        - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
        - TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
        - TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256
      sniStrict: true

http:
  middlewares:
    security-headers:
      headers:
        stsSeconds: 63072000
        stsIncludeSubdomains: true
        stsPreload: true
        contentTypeNosniff: true
        frameDeny: true
        browserXssFilter: true
        referrerPolicy: strict-origin-when-cross-origin
    rate-limit:
      rateLimit:
        average: 100
        burst: 50
        period: 1s
EOF

# Write Traefik docker-compose override
cat > docker-compose.traefik.yml << 'EOF'
# Traefik reverse proxy — run alongside docker-compose.quickstart.yml
# Usage: docker compose -f docker-compose.quickstart.yml -f docker-compose.traefik.yml --env-file .env up -d

name: rsync-ai

services:
  traefik:
    image: traefik:v3.0
    container_name: rsync-traefik
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./traefik/traefik.yml:/etc/traefik/traefik.yml:ro
      - ./traefik/dynamic.yml:/etc/traefik/dynamic.yml:ro
      - ./traefik/letsencrypt:/letsencrypt
    environment:
      - TRAEFIK_CERTIFICATESRESOLVERS_letsencrypt_ACME_EMAIL=${RSYNC_ADMIN_EMAILS}

  api-gateway:
    ports: !reset []
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.api.rule=Host(`${PUBLIC_HOST}`) && PathPrefix(`/api`, `/ws`)"
      - "traefik.http.routers.api.entrypoints=websecure"
      - "traefik.http.routers.api.tls.certresolver=letsencrypt"
      - "traefik.http.routers.api.middlewares=security-headers@file,rate-limit@file"
      - "traefik.http.services.api.loadbalancer.server.port=8080"

  frontend:
    ports: !reset []
    labels:
      - "traefik.enable=true"
      - "traefik.http.routers.frontend.rule=Host(`${PUBLIC_HOST}`)"
      - "traefik.http.routers.frontend.entrypoints=websecure"
      - "traefik.http.routers.frontend.tls.certresolver=letsencrypt"
      - "traefik.http.routers.frontend.middlewares=security-headers@file"
      - "traefik.http.services.frontend.loadbalancer.server.port=3000"
      - "traefik.http.routers.frontend.priority=1"
EOF
```

Replace `YOUR_ADMIN_EMAIL` in `traefik/traefik.yml` with your real email (used for Let's Encrypt notifications).

Then add `PUBLIC_HOST` to your `.env`:
```bash
echo "PUBLIC_HOST=app.yourdomain.com" >> ~/rsync-ai/.env
```

Start everything with Traefik:
```bash
cd ~/rsync-ai
docker compose \
  -f docker-compose.quickstart.yml \
  -f docker-compose.traefik.yml \
  --env-file .env \
  up -d
```

Traefik will automatically obtain a TLS certificate via Let's Encrypt. HTTP traffic is permanently redirected to HTTPS.

---

## Step 9 — Enable auto-start on reboot

```bash
sudo tee /etc/systemd/system/rsync-ai.service << EOF
[Unit]
Description=rsync.ai
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=$HOME/rsync-ai
ExecStart=docker compose -f docker-compose.quickstart.yml -f docker-compose.traefik.yml --env-file .env up -d
ExecStop=docker compose -f docker-compose.quickstart.yml down
User=$USER

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable rsync-ai
sudo systemctl start rsync-ai
```

> If you are **not** using a custom domain, skip Step 8 and the `docker-compose.traefik.yml` override. The quickstart compose exposes the frontend on port 3000 and api-gateway on port 5001 directly.

---

## Useful commands

```bash
# View logs
cd ~/rsync-ai && docker compose -f docker-compose.quickstart.yml logs -f

# View Traefik logs
docker logs rsync-traefik -f

# Stop
cd ~/rsync-ai && docker compose -f docker-compose.quickstart.yml -f docker-compose.traefik.yml down

# Update to latest version
cd ~/rsync-ai && docker compose -f docker-compose.quickstart.yml pull && \
  docker compose -f docker-compose.quickstart.yml -f docker-compose.traefik.yml --env-file .env up -d

# Check status
docker ps
```

---

## Resource usage on Oracle Free ARM (4 OCPU / 24 GB)

| Service | RAM |
|---|---|
| Kafka | ~512 MB |
| Temporal | ~512 MB |
| PostgreSQL | ~256 MB |
| Redis | ~128 MB |
| Traefik | ~64 MB |
| API Gateway | ~128 MB |
| Orchestrator | ~256 MB |
| LLM Service + Planner + Tool Generator | ~1 GB |
| Frontend | ~256 MB |
| MCP Connectors (4×) | ~256 MB |
| **Total** | **~3.5 GB** |

You have ~20 GB headroom — plenty for user pipelines and growth.
