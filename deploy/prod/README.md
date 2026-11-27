# Running the public demo

One VM, Docker Compose, Caddy for TLS. Everything Ithaca needs runs in
[`docker-compose.yml`](docker-compose.yml); only ports 80 and 443 face the
internet.

## Rehearse on your laptop first

The same stack with the same files, served at https://localhost from Caddy's local CA:

```bash
make prod-local      # builds the image, starts the stack
open https://localhost   # accept the local certificate once
make prod-local-down # stop it (add -v by hand to also drop its data)
```

## On a server

Any Linux VM with 2 vCPUs and 2–4 GB of RAM works. The image is built for amd64 and arm64.
Free or cheap options: Oracle Cloud's Always Free ARM VM, DigitalOcean with GitHub Student Pack
credit, or a ~€4/month Hetzner CX22.

1. **Create the VM** (Ubuntu 24.04). Allow inbound 22, 80 and 443 in the provider's firewall.
   *Oracle only:* its Ubuntu images also block 80/443 in the VM's own iptables rules, so open
   them there too.

2. **Install Docker** and let your user run it:
   ```bash
   curl -fsSL https://get.docker.com | sh
   sudo usermod -aG docker $USER && exit   # log back in afterwards
   ```

3. **Copy the three files** from this directory:
   ```bash
   mkdir ~/ithaca                                     # on the server
   scp deploy/prod/{docker-compose.yml,Caddyfile,.env.example} you@server:ithaca/   # from the repo
   ```

4. **Configure.** On the server, `cd ~/ithaca && cp .env.example .env`, then set:
   - `ITHACA_DOMAIN`: with no domain, use the server's IP with dashes plus `.sslip.io`, e.g.
     `203-0-113-7.sslip.io`. That is a real public hostname, so Let's Encrypt issues a real
     certificate. With a domain, point an A record at the server and use it here.
   - `POSTGRES_PASSWORD`: `openssl rand -hex 24`.

5. **Make the image pullable.** The first tag push creates the `ithaca` package on GitHub as
   private. Either set it to public (GitHub → your profile → Packages → ithaca → Package
   settings → Change visibility), or run `docker login ghcr.io` on the server with a read-only
   token.

6. **Start it:**
   ```bash
   docker compose up -d
   docker compose ps     # every service healthy within ~30 s
   ```
   Then open `https://<ITHACA_DOMAIN>`. The first request takes a few seconds while Caddy
   fetches the certificate.

## Releasing

```bash
git tag v0.1.0 && git push origin v0.1.0
```

`.github/workflows/deploy.yml` builds and pushes `ghcr.io/sharmasanskar/ithaca:v0.1.0` (and
`:latest`). If the repository has the `DEPLOY_*` secrets listed at the top of that workflow, it
also copies the compose files to the server, pins `ITHACA_VERSION` in its `.env`, rolls the stack
and waits for every service to report healthy. Without them it only pushes.

## Operating it

| | |
|---|---|
| logs | `docker compose logs -f dispatcher` |
| a psql shell | `docker compose exec postgres psql -U ithaca` |
| restart one service | `docker compose up -d --force-recreate api` |
| roll back | set `ITHACA_VERSION` in `.env` to the previous tag, then `docker compose up -d` |
| disk | delivery history older than `ITHACA_RETENTION` (6h) is deleted by the janitor |

The demo tenant's API key is printed once, by the `seed` job, to its logs (`docker compose logs
seed`). It can only manage endpoints that point at the built-in sinks.
