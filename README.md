# Hafen — Migration Platform for Escaping Legacy Oracle Workloads

Hafen helps enterprises migrate away from Oracle by converting PL/SQL packages, procedures, and triggers into idiomatic PostgreSQL — with AI-generated test harnesses that prove correctness.

**Phase 1 — Free Complexity Analyzer:** Upload a zip of Oracle DDL + PL/SQL, get a complexity score, effort estimate, and downloadable PDF report.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12 (for local development)
- Node.js 20+ (for local frontend dev)

### Run with Docker Compose

```bash
# Copy environment
cp .env.example .env

# Start services
docker compose up

# Services:
# - API: http://localhost:8000
# - Web: http://localhost:3000
# - Postgres: localhost:5432
```

### Run Tests

```bash
# API tests
cd apps/api
pip install -e ".[dev]"
pytest tests/

# Coverage report
pytest tests/ --cov=src --cov-report=html
```

### Local Development (without Docker)

**Backend:**
```bash
cd apps/api
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
uvicorn src.main:app --reload
```

**Frontend:**
```bash
cd apps/web
npm install
npm run dev
```

## Architecture

```
hafen/
├── apps/
│   ├── api/              # FastAPI backend (Python 3.12)
│   │   ├── src/
│   │   │   ├── main.py              # FastAPI routes
│   │   │   ├── parsers/
│   │   │   │   └── plsql_parser.py  # Oracle PL/SQL parser
│   │   │   ├── analyzers/
│   │   │   │   └── complexity_scorer.py  # Complexity analysis
│   │   │   ├── reports/
│   │   │   │   └── pdf_generator.py # ReportLab PDF output
│   │   │   ├── llm/
│   │   │   │   └── client.py        # Anthropic SDK (Phase 2)
│   │   │   ├── models.py            # SQLAlchemy models
│   │   │   └── db.py                # Database setup
│   │   └── tests/
│   │       ├── test_parser.py       # Parser tests
│   │       ├── test_complexity.py   # Scorer tests
│   │       └── fixtures/
│   │           └── hr_schema/       # Oracle HR schema fixture
│   └── web/              # Next.js 14 frontend (TypeScript)
│       ├── app/
│       │   ├── page.tsx             # Main upload page
│       │   └── components/
│       │       ├── UploadZone.tsx   # Drag-drop upload
│       │       └── ReportPreview.tsx # Results display
│       └── public/
├── docker-compose.yml   # Local dev environment
├── .env.example         # Environment template
└── README.md           # This file
```

## API Endpoints

### Analyze
```
POST /api/v1/analyze
Content-Type: multipart/form-data

Parameters:
  - file (zip): Oracle DDL + PL/SQL files
  - email (string): User email
  - rate_per_day (int, optional): $/engineer-day (default 1000)

Response:
  { "job_id": "uuid", "status": "processing" }
```

### Get Job Status
```
GET /api/v1/jobs/{job_id}

Response:
  {
    "id": "uuid",
    "status": "processing|done|error",
    "complexity_report": {
      "score": 45,
      "total_lines": 12500,
      "auto_convertible_lines": 7500,
      "needs_review_lines": 3000,
      "must_rewrite_lines": 2000,
      "construct_counts": {...},
      "effort_estimate_days": 5.5,
      "estimated_cost": 5500,
      "top_10_constructs": [...]
    }
  }
```

### Download PDF Report
```
GET /api/v1/report/{job_id}/pdf

Response: application/pdf
```

## Complexity Scoring

### Tier A — Auto-convertible (weight: 1)
- Procedures, functions, packages
- Basic triggers
- Views, sequences, tables
- Basic Oracle functions (DECODE → CASE, NVL → COALESCE, etc.)

### Tier B — Needs review (weight: 5)
- CONNECT BY (hierarchical queries)
- MERGE statements
- %TYPE / %ROWTYPE
- DBMS_OUTPUT
- Global temp tables
- VPD policies
- EXECUTE IMMEDIATE

### Tier C — Must rewrite (weight: 20)
- PRAGMA AUTONOMOUS_TRANSACTION
- DBMS_SCHEDULER jobs
- DBMS_AQ (Advanced Queuing)
- DBMS_CRYPTO
- Oracle Spatial
- Oracle Text
- Database links

### Score Formula
```
raw = (tier_b_constructs * 5 + tier_c_constructs * 20) / total_constructs
score = min(100, int(raw * 10 + log10(total_lines) * 5))
```

### Effort Estimation
```
auto_days    = auto_lines / 1000 * 0.1
review_days  = review_lines / 100 * 0.5
rewrite_days = rewrite_lines / 100 * 2.0
total_days   = ceil(auto_days + review_days + rewrite_days)
cost_estimate = total_days * rate_per_day  # default $1,000/day
```

## Development

### Adding New Constructs

Edit `src/parsers/plsql_parser.py`:

```python
# Add new construct type to ConstructType enum
class ConstructType(str, Enum):
    NEW_CONSTRUCT = "NEW_CONSTRUCT"

# Add detection regex in _find_special_constructs():
if re.search(r"NEW_PATTERN", content, re.IGNORECASE):
    self.constructs.append(Construct(
        type=ConstructType.NEW_CONSTRUCT,
        name="NEW_CONSTRUCT",
        ...
    ))

# Classify in tier_a/b/c_constructs sets
self.tier_b_constructs.add(ConstructType.NEW_CONSTRUCT)
```

### Running Tests

```bash
# All tests
pytest tests/

# Specific test
pytest tests/test_complexity.py::TestComplexityScorer::test_hr_schema

# With coverage
pytest tests/ --cov=src --cov-report=term-missing

# Watch mode
pytest-watch tests/
```

## Phase Roadmap

### Phase 1 (Week 1) — ✓ In Progress
- Free Complexity Analyzer
- ANTLR4-based PL/SQL parser
- Complexity scorer
- PDF report generator
- Next.js upload UI

### Phase 2 (Week 2-3)
- PL/SQL-to-PL/pgSQL converter
- Schema converter
- Side-by-side diff UI
- Stripe billing integration
- Developer ($199/mo) + Team ($999/mo) tiers

### Phase 3 (Week 4)
- pgTAP test harness generator
- Migration progress tracking
- Enterprise deployment ($25K–$100K/year)

### Phase 4 (Month 2-3)
- Consulting firm partnerships
- White-label licensing

## Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/hafen_dev
POSTGRES_PASSWORD=hafen_dev_pw

# LLM (Phase 2)
ANTHROPIC_API_KEY=sk-ant-...

# Server
ENVIRONMENT=development
API_HOST=0.0.0.0
API_PORT=8000
```

## Known Limitations

- Phase 1 uses regex-based parsing; ANTLR4 integration planned for Phase 2
- Synchronous analysis (zip < 10MB); async queue (Celery/ARQ) in Phase 2
- 70% automation target (honest over false 100%)
- PDF reports require 2–3 pages minimum

## Support

- GitHub Issues: [Issues](https://github.com/yourname/hafen/issues)
- Email: support@hafen.ai (Phase 2+)

## Legal

This tool is designed for **migration off Oracle**, not Oracle replacement. Always use Oracle-neutral language in marketing and respect Oracle's trademarks. See `docs/LEGAL_NOTES.md` for details.

## License

Proprietary — See LICENSE file
