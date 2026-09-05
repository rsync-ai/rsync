# ACM/ELv2 TLS Certificate

"ELv2 certificate" refers to a TLS certificate issued by AWS Certificate Manager (ACM) and attached
to an Application Load Balancer (ALBv2). It is free for public certificates and auto-renews.

---

## Step 1 — Request the certificate

**AWS Console → Certificate Manager → Request → Public certificate**

| Field | Value |
|---|---|
| Domain name | `yourdomain.com` |
| Additional name | `*.yourdomain.com` (covers all subdomains) |
| Validation method | DNS validation (recommended) |
| Key algorithm | RSA 2048 |

Click **Request**.

---

## Step 2 — DNS validation via Route 53

ACM gives you a CNAME record to prove domain ownership.

**If your domain is in Route 53**:
- Click **"Create records in Route 53"** — one click, adds the CNAME automatically
- Status changes from `Pending validation` to `Issued` in 5–30 minutes

**If your domain is on another registrar** (Namecheap, GoDaddy, etc.):
- Copy the CNAME name and value from ACM
- Add it as a CNAME record in your registrar's DNS panel
- Wait up to 30 minutes for DNS propagation

The CNAME must stay in DNS permanently — ACM uses it for auto-renewal.

---

## Step 3 — Create the Application Load Balancer

**EC2 Console → Load Balancers → Create load balancer → Application Load Balancer**

| Setting | Value |
|---|---|
| Name | `rsync-ai-alb` |
| Scheme | Internet-facing |
| IP type | IPv4 |
| VPC | Your VPC |
| Subnets | Both public subnets (at least 2 AZs) |
| Security group | `alb-sg` (ports 80 and 443 from `0.0.0.0/0`) |

---

## Step 4 — Add HTTP → HTTPS redirect listener

**Listeners → Add listener**

| Setting | Value |
|---|---|
| Protocol | HTTP |
| Port | 80 |
| Default action | Redirect to HTTPS, port 443, status 301 |

---

## Step 5 — Add HTTPS listener with ACM certificate

**Listeners → Add listener**

| Setting | Value |
|---|---|
| Protocol | HTTPS |
| Port | 443 |
| Default action | Forward to `rsync-frontend` target group |
| Certificate source | ACM |
| Certificate | Select the cert from Step 1 |
| Security policy | `ELBSecurityPolicy-TLS13-1-2-2021-06` (TLS 1.2+) |

---

## Step 6 — Add listener rules

On the HTTPS :443 listener, add rules (evaluated top to bottom):

| Priority | Condition | Action |
|---|---|---|
| 1 | Path pattern: `/api/*` | Forward to `rsync-api` target group |
| 2 | Path pattern: `/ws` | Forward to `rsync-api` target group |
| 3 | Path pattern: `/oauth/callback/*` | Forward to `rsync-api` target group |
| Last | Default | Forward to `rsync-frontend` target group |

WebSocket connections work through ALB with no extra configuration — ALB handles the upgrade.

---

## Step 7 — Route 53 A record → ALB

**Route 53 → Hosted zone → Create record**

| Field | Value |
|---|---|
| Record name | `yourdomain.com` |
| Type | A |
| Alias | Yes |
| Alias target | ALB DNS name (e.g. `rsync-ai-alb-xxx.us-east-1.elb.amazonaws.com`) |

Repeat for `www.yourdomain.com` pointing to the same ALB.

---

## Step 8 — Verify

```bash
# Should return 301 → redirect to HTTPS
curl -I http://yourdomain.com

# Should return 200 with valid TLS
curl https://yourdomain.com/health

# Check certificate details
openssl s_client -connect yourdomain.com:443 -servername yourdomain.com 2>/dev/null \
  | openssl x509 -noout -subject -dates
```

---

## Certificate auto-renewal

ACM certificates auto-renew as long as:
1. The DNS validation CNAME record stays in Route 53
2. The cert is in use (attached to an ALB listener)

ACM will attempt renewal 60 days before expiry. No manual action needed.

---

## Traefik conflict

The `docker-compose.prod.yml` in rsync-ai includes Traefik with its own Let's Encrypt configuration.
Since ALB now handles TLS termination, **disable Traefik** to avoid port conflicts:

```yaml
# In your override or docker-compose.prod.yml:
  traefik:
    profiles: ["traefik-local"]   # only starts when explicitly profiled
```

Or simply don't include `docker-compose.prod.yml` on EC2 — use a separate production override
that only sets environment variables and resource limits, not Traefik.

---

## Common errors

| Error | Cause | Fix |
|---|---|---|
| Certificate stays `Pending validation` > 30 min | CNAME not propagated | Check CNAME with `dig CNAME _acme-challenge.yourdomain.com` |
| `SSL_ERROR_RX_RECORD_TOO_LONG` in browser | HTTP traffic hitting HTTPS port | Check listener redirect rule |
| `502 Bad Gateway` | EC2/ECS not listening on target group port | Check security group + health check |
| Certificate not covering subdomain | Wildcard not added | Add `*.yourdomain.com` to cert |
