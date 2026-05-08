# ── Stage 1: compile moat modules ────────────────────────────────────────────
# Produces .so extensions for the IP-sensitive packages.  Only gcc +
# python3-dev + Cython needed here; runtime deps are in stage 2.
FROM python:3.12-slim AS compiler
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install "cython>=3.0" setuptools

COPY src/ ./src/
COPY scripts/build_cython.py ./scripts/

RUN python3 scripts/build_cython.py


# ── Stage 2: runtime image ───────────────────────────────────────────────────
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
        # SAML stack: python3-saml's xmlsec native dep.
        libxmlsec1-openssl \
        libxmlsec1-dev \
        libxml2-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install -e .

# Full Python source tree (non-moat modules + __init__.py files).
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY alembic/ ./alembic/
COPY alembic.ini ./

# Generate ANTLR PL/SQL parser (jar cached at build time; layer cached
# unless grammar/ changes).
RUN python3 scripts/generate_grammar.py

# Overlay compiled .so files from the compiler stage, then strip the
# corresponding .py sources so moat code ships as binary only.
COPY --from=compiler /build/src/ ./src/
RUN python3 scripts/strip_sources.py

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD sh -c "alembic upgrade head && uvicorn src.main:app --host 0.0.0.0 --port 8000"
