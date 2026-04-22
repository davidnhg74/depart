# Depart Platform - Deployment Guide

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
export DATABASE_URL=postgresql://user:pass@localhost:5432/depart

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
psql -U depart_user -d depart -h localhost < apps/api/migrations/init.sql
```

## Production Deployment

### 1. Configure Environment

```bash
# .env for production
ENVIRONMENT=production
DATABASE_URL=postgresql://user:pass@prod-db:5432/depart
ANTHROPIC_API_KEY=sk-...
```

### 2. Build Images

```bash
docker-compose -f docker-compose.yml build
```

### 3. Push to Registry

```bash
docker push myregistry/depart-api:latest
docker push myregistry/depart-web:latest
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
docker-compose exec postgres psql -U depart_user -d depart -c "SELECT 1"
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
docker-compose exec postgres pg_dump -U depart_user depart > backup.sql
```

### Restore Database

```bash
docker-compose exec -T postgres psql -U depart_user depart < backup.sql
```

## Scaling Considerations

- Run API instances behind a load balancer
- Use managed PostgreSQL for production
- Implement caching layer (Redis)
- Monitor metrics with Prometheus/Grafana
- Set up alerts for error rates and latency
