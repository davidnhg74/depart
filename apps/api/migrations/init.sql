-- Depart Platform - Initial Database Schema
-- PostgreSQL migration for Phase 1-3 features

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgvector";

-- Phase 1: Leads and Analysis Jobs
CREATE TABLE IF NOT EXISTS leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_email (email)
);

CREATE TABLE IF NOT EXISTS analysis_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    complexity_report JSONB,
    pdf_path VARCHAR(512),
    rate_per_day INTEGER DEFAULT 1000,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    INDEX idx_lead_id (lead_id),
    INDEX idx_status (status)
);

-- Phase 3: RAG System
CREATE TABLE IF NOT EXISTS conversion_cases (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    construct_type VARCHAR(50) NOT NULL,
    oracle_code TEXT NOT NULL,
    postgres_code TEXT NOT NULL,
    embedding FLOAT8[] NOT NULL,
    success_count INTEGER DEFAULT 1,
    fail_count INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_construct_type (construct_type),
    INDEX idx_created_at (created_at)
);

CREATE INDEX IF NOT EXISTS idx_conversion_cases_embedding ON conversion_cases USING ivfflat (embedding vector_cosine_ops);

-- Phase 3: Migrations
CREATE TABLE IF NOT EXISTS migrations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schema_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    total_rows INTEGER DEFAULT 0,
    rows_transferred INTEGER DEFAULT 0,
    total_bytes INTEGER DEFAULT 0,
    estimated_duration_seconds INTEGER,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- Phase 3: Migration Checkpoints
CREATE TABLE IF NOT EXISTS migration_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    migration_id UUID NOT NULL REFERENCES migrations(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    rows_processed INTEGER DEFAULT 0,
    total_rows INTEGER DEFAULT 0,
    progress_percentage FLOAT DEFAULT 0.0,
    last_rowid VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_migration_id (migration_id),
    INDEX idx_created_at (created_at)
);

-- Phase 3: Migration Workflows
CREATE TABLE IF NOT EXISTS migration_workflows (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    migration_id UUID REFERENCES migrations(id) ON DELETE SET NULL,
    current_step INTEGER DEFAULT 1,
    status VARCHAR(50) DEFAULT 'running',
    dba_notes JSONB DEFAULT '{}',
    approvals JSONB DEFAULT '{}',
    settings JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
);

-- Phase 3: Benchmark Captures
CREATE TABLE IF NOT EXISTS benchmark_captures (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    migration_id UUID REFERENCES migrations(id) ON DELETE SET NULL,
    db_type VARCHAR(20) NOT NULL,
    captured_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_migration_id (migration_id),
    INDEX idx_captured_at (captured_at)
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO depart_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO depart_user;
