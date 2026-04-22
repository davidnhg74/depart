#!/usr/bin/env bash
# Build an air-gapped installer bundle for depart.
#
# Produces a single tarball the operator copies to the target host,
# extracts, and runs — no internet access required at install time.
# Contents:
#
#   depart-airgap/
#     README.md                 — step-by-step install + first-migration guide
#     docker-compose.yml        — the self-hosted stack
#     images/
#       postgres.tar            — pgvector/pgvector:pg16
#       api.tar                 — depart-api:<tag>
#       web.tar                 — depart-web:<tag>
#     install.sh                — loads images, brings the stack up, sanity-checks
#     uninstall.sh              — brings the stack down, removes images
#     fixtures/
#       oracle-init/            — HR fixture schema (optional Oracle container)
#     .env.example              — operator copies to .env and fills in
#
# Enterprise tier ships this bundle instead of pointing at Docker Hub.
# Every image is pre-pulled; the install script uses `docker load` so no
# registry pulls happen at the target site.
#
# Usage:
#   scripts/build-airgap-bundle.sh [--tag v0.2.0] [--output dist/]

set -euo pipefail

# ─── Config ────────────────────────────────────────────────────────────────

TAG="${TAG:-airgap-$(date -u +%Y%m%d)}"
OUTPUT_DIR="${OUTPUT_DIR:-$(pwd)/dist}"
BUNDLE_NAME="depart-airgap-${TAG}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

while [[ $# -gt 0 ]]; do
    case $1 in
        --tag) TAG="$2"; BUNDLE_NAME="depart-airgap-${TAG}"; shift 2 ;;
        --output) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)
            grep '^#' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 1 ;;
    esac
done

STAGING="${OUTPUT_DIR}/${BUNDLE_NAME}"

echo "→ building bundle ${BUNDLE_NAME} in ${OUTPUT_DIR}"
rm -rf "${STAGING}"
mkdir -p "${STAGING}/images"

# ─── 1. Build the API + web images ─────────────────────────────────────────

echo "→ building depart-api image..."
docker build -t "depart-api:${TAG}" "${REPO_ROOT}/apps/api"

echo "→ building depart-web image..."
docker build -t "depart-web:${TAG}" "${REPO_ROOT}/apps/web"

# ─── 2. Pull the third-party images we depend on ──────────────────────────

echo "→ pulling pgvector/pgvector:pg16..."
docker pull pgvector/pgvector:pg16

# ─── 3. Save all images to tar files (docker load on target) ──────────────

echo "→ exporting images to tarballs..."
docker save -o "${STAGING}/images/postgres.tar" pgvector/pgvector:pg16
docker save -o "${STAGING}/images/api.tar" "depart-api:${TAG}"
docker save -o "${STAGING}/images/web.tar" "depart-web:${TAG}"

# ─── 4. Copy compose + fixtures ───────────────────────────────────────────

echo "→ copying compose + fixtures + readme..."
# Generate an air-gap-flavored compose that references the :local tag
# we load on the target, not the upstream Docker Hub names.
cat > "${STAGING}/docker-compose.yml" <<EOF
# depart — air-gap compose. All image tags resolve to images you load
# from ./images/*.tar via install.sh — no registry pulls at runtime.

services:
  postgres:
    image: pgvector/pgvector:pg16
    container_name: depart_postgres
    environment:
      POSTGRES_DB: depart
      POSTGRES_USER: depart_user
      POSTGRES_PASSWORD: \${DB_PASSWORD:-depart_secure_password}
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --lc-collate=C --lc-ctype=C"
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U depart_user -d depart"]
      interval: 5s
      timeout: 5s
      retries: 10
    networks: [depart_network]

  api:
    image: depart-api:${TAG}
    container_name: depart_api
    environment:
      DATABASE_URL: postgresql+psycopg://depart_user:\${DB_PASSWORD:-depart_secure_password}@postgres:5432/depart
      ENABLE_CLOUD_ROUTES: "false"
      ANTHROPIC_API_KEY: \${ANTHROPIC_API_KEY:-}
      ENVIRONMENT: production
      API_HOST: 0.0.0.0
      API_PORT: "8000"
    ports: ["8000:8000"]
    depends_on:
      postgres: { condition: service_healthy }
    networks: [depart_network]
    restart: unless-stopped

  web:
    image: depart-web:${TAG}
    container_name: depart_web
    ports: ["3000:3000"]
    environment:
      NEXT_PUBLIC_API_URL: \${NEXT_PUBLIC_API_URL:-http://localhost:8000}
    depends_on: [api]
    networks: [depart_network]
    restart: unless-stopped

volumes:
  postgres_data:

networks:
  depart_network:
EOF

cp -R "${REPO_ROOT}/docker/oracle-init" "${STAGING}/fixtures/oracle-init" 2>/dev/null || true

cat > "${STAGING}/.env.example" <<'EOF'
# depart air-gap config. Copy to `.env` and fill in.
#
# DB_PASSWORD:         any strong password; only used internally.
# ANTHROPIC_API_KEY:   optional. Needed for live AI conversion. Can also
#                      be set at runtime via http://localhost:3000/settings/instance.
# NEXT_PUBLIC_API_URL: the URL the browser uses to reach the API. Localhost
#                      is fine for single-machine installs.

DB_PASSWORD=change-me-to-something-secret
ANTHROPIC_API_KEY=
NEXT_PUBLIC_API_URL=http://localhost:8000
EOF

# ─── 5. Installer script ──────────────────────────────────────────────────

cat > "${STAGING}/install.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "→ loading Docker images..."
docker load -i images/postgres.tar
docker load -i images/api.tar
docker load -i images/web.tar

if [[ ! -f .env ]]; then
    cp .env.example .env
    echo "→ created .env from template — edit it and re-run if you want to customize."
fi

echo "→ bringing the stack up..."
docker compose up -d

echo "→ waiting for the API to become ready..."
for _ in $(seq 1 30); do
    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        echo "→ API ready."
        echo
        echo "  Open http://localhost:3000 in your browser."
        exit 0
    fi
    sleep 2
done

echo "!! API did not come up in 60s — check 'docker compose logs api'" >&2
exit 1
EOF

cat > "${STAGING}/uninstall.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "→ stopping the stack..."
docker compose down

echo "→ removing images..."
docker image rm -f pgvector/pgvector:pg16 depart-api:$(grep -oP 'depart-api:\K[^ ]+' docker-compose.yml | head -1) depart-web:$(grep -oP 'depart-web:\K[^ ]+' docker-compose.yml | head -1) 2>/dev/null || true
echo "→ done. Postgres volume (depart_postgres_data) left in place — remove it manually if you want a clean slate."
EOF

chmod +x "${STAGING}/install.sh" "${STAGING}/uninstall.sh"

# ─── 6. README ────────────────────────────────────────────────────────────

cat > "${STAGING}/README.md" <<EOF
# depart — air-gap install bundle (${TAG})

Everything in this tarball is self-contained. No internet access needed
on the target host after extraction.

## Requirements

- Docker 24+
- 4 GB RAM, 10 GB disk
- Ports 3000 (web) and 8000 (api) free

## Install

\`\`\`bash
tar -xzf depart-airgap-${TAG}.tar.gz
cd depart-airgap-${TAG}
cp .env.example .env      # optional: edit to customize passwords
./install.sh
\`\`\`

Open <http://localhost:3000>. Go to **Settings → Instance settings** to:
1. Upload your license JWT (unlocks AI conversion + runbook PDF)
2. Optionally paste your Anthropic API key (for BYOK AI conversion)

## First migration

See <http://localhost:3000/assess> — paste your Oracle DDL, get a
complexity report, and drill into each risk for side-by-side
Oracle → Postgres conversion samples.

## Uninstall

\`\`\`bash
./uninstall.sh
\`\`\`

This stops the stack and removes the loaded images. The Postgres
volume is left in place; remove it with \`docker volume rm depart_postgres_data\`
if you want a clean slate.

## Support

Enterprise license holders: email <support@depart.io> with the project
and license subject shown at **Settings → Instance settings**. We
respond within SLA even for air-gapped deployments (send us the
runbook PDF or logs out-of-band).
EOF

# ─── 7. Pack ──────────────────────────────────────────────────────────────

echo "→ packing tarball..."
tar -czf "${OUTPUT_DIR}/${BUNDLE_NAME}.tar.gz" -C "${OUTPUT_DIR}" "${BUNDLE_NAME}"
SIZE=$(du -h "${OUTPUT_DIR}/${BUNDLE_NAME}.tar.gz" | awk '{print $1}')

echo
echo "✓ built ${OUTPUT_DIR}/${BUNDLE_NAME}.tar.gz (${SIZE})"
echo "  ship this to the air-gapped host, extract, and run ./install.sh"
