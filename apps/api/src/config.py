from pydantic_settings import BaseSettings
from pydantic import ConfigDict
import os


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="allow")

    # Database — must specify the +psycopg driver explicitly. Bare "postgresql://"
    # makes SQLAlchemy reach for psycopg2, which we don't ship; psycopg (v3) is
    # in the dependency set.
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://hafen_user:hafen_secure_password@localhost:5432/hafen",
    )

    # Redis URL for the arq migration worker. localhost default lets
    # the API run standalone in dev; docker-compose sets this to
    # redis://redis:6379/0.
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Environment
    environment: str = os.getenv("ENVIRONMENT", "development")
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    max_upload_size: int = int(os.getenv("MAX_UPLOAD_SIZE", str(50 * 1024 * 1024)))
    frontend_url: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

    # Cloud routes (signup, billing, support, email verification) default
    # to OFF so the self-hosted product image boots without Stripe /
    # Resend / SaaS-signup exposure. The marketing/purchase site
    # (hafen.ai) flips this to True. Tests enable it — see
    # tests/conftest.py.
    enable_cloud_routes: bool = os.getenv("ENABLE_CLOUD_ROUTES", "false").lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    # Self-hosted auth (login, session, admin-gated user CRUD) defaults
    # ON. Every enterprise deployment needs authentication — "localhost
    # = admin" is not acceptable on internal VLANs. This flag exists
    # so a single-user dev box can flip it off explicitly with
    # ENABLE_SELF_HOSTED_AUTH=false, not so production installs can.
    enable_self_hosted_auth: bool = os.getenv(
        "ENABLE_SELF_HOSTED_AUTH", "true"
    ).lower() in ("1", "true", "yes", "on")

    # AI/LLM
    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")

    # JWT Authentication
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "change-me-in-production-super-secret-key")
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    # Stripe Billing
    stripe_secret_key: str = os.getenv("STRIPE_SECRET_KEY", "")
    stripe_webhook_secret: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")
    stripe_price_ids: dict = {
        "starter": os.getenv("STRIPE_PRICE_STARTER", ""),
        "professional": os.getenv("STRIPE_PRICE_PROFESSIONAL", ""),
        "enterprise": os.getenv("STRIPE_PRICE_ENTERPRISE", ""),
    }

    # Email (Resend)
    resend_api_key: str = os.getenv("RESEND_API_KEY", "")
    support_email: str = os.getenv("SUPPORT_EMAIL", "support@hafen.ai")


settings = Settings()
