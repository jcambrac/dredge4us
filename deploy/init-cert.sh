#!/usr/bin/env bash
# One-time Let's Encrypt issuance for ${API_DOMAIN}, run via `make cert`
# from the repo root before the first `make deploy`. Uses certbot's
# standalone server on port 80, so nginx must not be running yet (this
# script stops it if it is). After this, the certbot service renews on
# its own through nginx.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a && source .env && set +a
: "${API_DOMAIN:?set API_DOMAIN in .env}"

compose=(docker compose --env-file .env -f deploy/docker-compose.yml)

"${compose[@]}" stop nginx 2>/dev/null || true
"${compose[@]}" run --rm -p 80:80 --entrypoint certbot certbot \
  certonly --standalone \
  -d "$API_DOMAIN" \
  --agree-tos --register-unsafely-without-email --non-interactive
