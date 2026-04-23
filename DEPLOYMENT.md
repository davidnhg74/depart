# Hafen Platform - Deployment Guide

## Prerequisites

- Docker & Docker Compose 20.10+
- PostgreSQL 15+ (if not using Docker)
- Python 3.14+ (for local development)
- Node.js 18+ (for web frontend)

## Quick Start with Docker

### 1. Environment Setup

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 2. Build and Run

```bash
docker-compose up -d
```

This will start:
- PostgreSQL database on port 5432
- API server on port 8000
- Web frontend on port 3000

### 3. Verify Health

```bash
curl http://localhost:8000/health
curl http://localhost:3000
```

## Local Development Setup

### API Setup

```bash
cd apps/api
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -e .

# Set environment
export DATABASE_URL=postgresql://user:pass@localhost:5432/hafen

# Run tests
pytest tests/ -v

# Start dev server
uvicorn src.main:app --reload
```

### Web Setup

```bash
cd apps/web
npm install
npm run dev
```

## Database Migrations

### Initial Setup

```bash
docker-compose exec api python -c "from src.db import create_tables; create_tables()"
```

### Manual Migration

```bash
psql -U hafen_user -d hafen -h localhost < apps/api/migrations/init.sql
```

## Tenancy modes

The same `apps/api` image runs in two modes, controlled by
`ENABLE_SELF_HOSTED_AUTH`:

| Mode | When | Tenancy behavior |
|---|---|---|
| **Single-tenant (self-hosted)** | `ENABLE_SELF_HOSTED_AUTH=false` (the default for the downloaded bundle) | Auth is a no-op; `caller` is `None`. Migrations, troubleshoot rows, audit events all carry `user_id IS NULL`. Listing/reading endpoints return everything in the install — appropriate for one operator owning the whole box. |
| **Multi-tenant (cloud SaaS)** | `ENABLE_SELF_HOSTED_AUTH=true` (set by `apps/api/fly.toml` for hafen-api) | Every request resolves to an authenticated `User`. New rows on `migrations`, `troubleshoot_analyses`, etc. are stamped with `caller.id`. Read/write endpoints filter by `MigrationRecord.user_id == caller.id` via `_load_or_404_for_user` — cross-tenant access returns 404 (never 403, to avoid revealing existence). |

When deploying the cloud SaaS, ALSO set:
- `ENABLE_CLOUD_ROUTES=true` to mount signup/billing/support/cloud_analyze
- `FRONTEND_URL=https://hafen.ai` (or your origin) for CORS + email links
- A real `JWT_SECRET_KEY` (don't ship the default literal)
- Stripe + Resend + Anthropic keys when those features are needed
  (the stack runs in a graceful-degraded mode without them — billing
  endpoints 503, emails no-op, troubleshoot returns a canned
  "AI temporarily unavailable" response)

The first admin user can be auto-created at startup via:
- `HAFEN_ADMIN_EMAIL=...`
- `HAFEN_ADMIN_PASSWORD=...`

`maybe_bootstrap_from_env` runs once on container start; if those
env vars are set AND no admin exists, the user is created with
`role=admin`, `email_verified=true`. Idempotent on re-restart.

## Production Deployment

### 1. Configure Environment

```bash
# .env for production
ENVIRONMENT=production
DATABASE_URL=postgresql://user:pass@prod-db:5432/hafen
ANTHROPIC_API_KEY=sk-...
```

### 2. Build Images

```bash
docker-compose -f docker-compose.yml build
```

### 3. Push to Registry

```bash
docker push myregistry/hafen-api:latest
docker push myregistry/hafen-web:latest
```

### 4. Deploy to K8s (Optional)

```bash
kubectl apply -f k8s/
```

## Monitoring

### Health Checks

```bash
# API health
curl http://localhost:8000/health

# DB connection
docker-compose exec api python -c "from src.db import get_engine; get_engine().execute('SELECT 1')"
```

### Logs

```bash
# API logs
docker-compose logs -f api

# DB logs
docker-compose logs -f postgres

# Web logs
docker-compose logs -f web
```

## Troubleshooting

### Database Connection Error

```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Verify credentials in .env
docker-compose exec postgres psql -U hafen_user -d hafen -c "SELECT 1"
```

### API Won't Start

```bash
# Check for port conflicts
lsof -i :8000

# View detailed logs
docker-compose logs api --tail=100
```

### Frontend Issues

```bash
# Clear Next.js cache
rm -rf apps/web/.next

# Rebuild
docker-compose up --build web
```

## Security Checklist

- [ ] Change default database password in .env
- [ ] Set ANTHROPIC_API_KEY if using LLM features
- [ ] Configure CORS for web domain
- [ ] Enable HTTPS in reverse proxy (nginx/traefik)
- [ ] Set up backup strategy for PostgreSQL
- [ ] Monitor logs for errors and anomalies
- [ ] Keep Docker images up to date
- [ ] Configure resource limits in docker-compose

## Performance Tuning

### Database

```sql
-- Enable pgvector indexing
CREATE INDEX idx_conversion_cases_embedding ON conversion_cases 
USING ivfflat (embedding vector_cosine_ops);

-- Analyze query plans
EXPLAIN ANALYZE SELECT * FROM migrations WHERE status = 'in_progress';
```

### API

- Use connection pooling (configured in db.py)
- Enable gzip compression in FastAPI
- Configure reasonable timeouts
- Monitor memory usage

### Web

- Enable Next.js static optimization
- Configure CDN for assets
- Use caching headers appropriately

## Backup and Recovery

### Backup Database

```bash
docker-compose exec postgres pg_dump -U hafen_user hafen > backup.sql
```

### Restore Database

```bash
docker-compose exec -T postgres psql -U hafen_user hafen < backup.sql
```

## Scaling Considerations

- Run API instances behind a load balancer
- Use managed PostgreSQL for production
- Implement caching layer (Redis)
- Monitor metrics with Prometheus/Grafana
- Set up alerts for error rates and latency
