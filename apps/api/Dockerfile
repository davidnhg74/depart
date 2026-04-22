FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gcc \
        libpq5 \
        default-jre-headless \
        # SAML stack: python3-saml's `xmlsec` native dep needs these.
        # xmlsec1 for runtime; libxmlsec1-dev + pkg-config + libxml2-dev
        # for the wheel build on platforms where no binary wheel exists.
        libxmlsec1-openssl \
        libxmlsec1-dev \
        libxml2-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install -e .

COPY src/ ./src/
COPY scripts/ ./scripts/
COPY alembic/ ./alembic/
COPY alembic.ini ./

# Generate the ANTLR PL/SQL parser. Cached jar lives in /root/.cache; this
# layer rebuilds only when grammar/ changes.
RUN python3 scripts/generate_grammar.py

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Single source of truth for schema is Alembic.
CMD sh -c "alembic upgrade head && uvicorn src.main:app --host 0.0.0.0 --port 8000"
