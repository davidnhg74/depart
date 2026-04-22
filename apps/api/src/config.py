from pydantic_settings import BaseSettings
from pydantic import ConfigDict
import os


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="allow")

    # Database
    database_url: str = os.getenv("DATABASE_URL", "postgresql://depart:depart_dev_pw@localhost:5432/depart_dev")

    # Environment
    environment: str = os.getenv("ENVIRONMENT", "development")
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    max_upload_size: int = 10 * 1024 * 1024  # 10MB
    frontend_url: str = os.getenv("FRONTEND_URL", "http://localhost:3000")

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
    support_email: str = os.getenv("SUPPORT_EMAIL", "support@depart.io")


settings = Settings()
