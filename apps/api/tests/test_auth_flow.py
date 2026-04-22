"""End-to-end integration tests for the signup → verify → login → reset flow.

Mounts just the auth router on a fresh FastAPI app per test, with the
real `get_db` dep overridden to a SQLite-backed in-memory session.
`send_verification_email` and `send_password_reset_email` are patched
so we capture the token instead of trying to talk to Resend.

This locks the contract that the frontend `lib/api.ts` helpers depend
on — every drift between the auth router and the helpers gets caught
here.
"""

from datetime import datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


# Force a non-empty JWT secret BEFORE the auth modules import settings.
@pytest.fixture(autouse=True)
def _set_jwt_secret(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "test-secret-key-not-for-prod")
    from src import config

    monkeypatch.setattr(
        config.settings, "jwt_secret_key", "test-secret-key-not-for-prod", raising=False
    )
    yield


# ─── DB fixture: SQLite in-memory, schema from User-related ORM models ──────


@pytest.fixture
def db_session():
    """A fresh in-memory SQLite session per test, with the ORM tables the
    auth router touches. Uses StaticPool so the same connection is shared
    across the engine — :memory: per-connection would lose state."""
    from src.db import Base
    from src.models import (
        AnalysisJob,
        ApiKey,
        Lead,
        SupportTicket,
        TicketMessage,
        Subscription,
        User,
    )

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # Only create the tables the auth flow touches. Other ORM models
    # (conversion_cases with pgvector ARRAY, etc.) don't compile on
    # sqlite — explicit table list keeps the test DB minimal.
    Base.metadata.create_all(
        bind=engine,
        tables=[
            Lead.__table__,
            User.__table__,
            ApiKey.__table__,
            Subscription.__table__,
            SupportTicket.__table__,
            TicketMessage.__table__,
            AnalysisJob.__table__,
        ],
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


# ─── Captured-email patches ──────────────────────────────────────────────────


@pytest.fixture
def captured_emails():
    """Patches send_verification_email + send_password_reset_email and
    returns a dict of {kind: [(email, token), ...]} populated by the tests."""
    bucket = {"verify": [], "reset": []}

    def _verify(email, token, frontend_url):
        bucket["verify"].append((email, token))
        return True

    def _reset(email, token, frontend_url):
        bucket["reset"].append((email, token))
        return True

    with (
        patch("src.routers.auth.send_verification_email", side_effect=_verify),
        patch("src.routers.auth.send_password_reset_email", side_effect=_reset),
    ):
        yield bucket


# ─── App + client ────────────────────────────────────────────────────────────


@pytest.fixture
def client(db_session):
    from src.routers.auth import router
    from src.db import get_db

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db_session
    return TestClient(app)


# ─── Signup ──────────────────────────────────────────────────────────────────


class TestSignup:
    def test_creates_user_and_returns_tokens(self, client, db_session, captured_emails):
        from src.models import User

        resp = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "alice@example.com",
                "full_name": "Alice",
                "password": "supersecret123",
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["token_type"] == "bearer"
        assert body["access_token"] and body["refresh_token"]

        # User row exists, password is hashed (not stored plaintext).
        user = db_session.query(User).filter(User.email == "alice@example.com").one()
        assert user.full_name == "Alice"
        assert user.hashed_password != "supersecret123"
        assert user.email_verified is False
        assert user.email_verify_token is not None

        # Verification email was "sent" with the same token.
        assert len(captured_emails["verify"]) == 1
        sent_email, sent_token = captured_emails["verify"][0]
        assert sent_email == "alice@example.com"
        assert sent_token == user.email_verify_token

    def test_duplicate_email_returns_409(self, client, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "dup@example.com",
                "full_name": "X",
                "password": "pw12345678",
            },
        )
        resp = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "dup@example.com",
                "full_name": "Y",
                "password": "pw12345678",
            },
        )
        assert resp.status_code == 409
        assert "already" in resp.json()["detail"].lower()

    def test_invalid_email_returns_422(self, client, captured_emails):
        resp = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "not-an-email",
                "full_name": "X",
                "password": "pw12345678",
            },
        )
        assert resp.status_code == 422


# ─── Verify email ───────────────────────────────────────────────────────────


class TestVerifyEmail:
    def test_valid_token_marks_verified(self, client, db_session, captured_emails):
        from src.models import User

        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "v@example.com",
                "full_name": "V",
                "password": "pw12345678",
            },
        )
        _, token = captured_emails["verify"][0]

        resp = client.post("/api/v4/auth/verify-email", json={"token": token})
        assert resp.status_code == 200

        # Re-query and check verification state cleared.
        db_session.expire_all()
        user = db_session.query(User).filter(User.email == "v@example.com").one()
        assert user.email_verified is True
        assert user.email_verify_token is None

    def test_invalid_token_returns_400(self, client):
        resp = client.post("/api/v4/auth/verify-email", json={"token": "totally-fake"})
        assert resp.status_code == 400

    def test_token_is_single_use(self, client, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "single@example.com",
                "full_name": "X",
                "password": "pw12345678",
            },
        )
        _, token = captured_emails["verify"][0]
        client.post("/api/v4/auth/verify-email", json={"token": token})

        # Second use must fail.
        resp = client.post("/api/v4/auth/verify-email", json={"token": token})
        assert resp.status_code == 400


# ─── Login ──────────────────────────────────────────────────────────────────


class TestLogin:
    def test_correct_credentials_return_tokens(self, client, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "login@example.com",
                "full_name": "L",
                "password": "loginpw1234",
            },
        )
        resp = client.post(
            "/api/v4/auth/login",
            json={
                "email": "login@example.com",
                "password": "loginpw1234",
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["token_type"] == "bearer"
        assert body["access_token"] and body["refresh_token"]

    def test_wrong_password_returns_401(self, client, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "wrong@example.com",
                "full_name": "W",
                "password": "rightpw1234",
            },
        )
        resp = client.post(
            "/api/v4/auth/login",
            json={
                "email": "wrong@example.com",
                "password": "wrongpw1234",
            },
        )
        assert resp.status_code == 401

    def test_unknown_email_returns_401(self, client):
        resp = client.post(
            "/api/v4/auth/login",
            json={
                "email": "ghost@example.com",
                "password": "anything12",
            },
        )
        assert resp.status_code == 401

    def test_inactive_user_returns_403(self, client, db_session, captured_emails):
        from src.models import User

        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "inactive@example.com",
                "full_name": "I",
                "password": "inactivepw1",
            },
        )
        user = db_session.query(User).filter(User.email == "inactive@example.com").one()
        user.is_active = False
        db_session.commit()

        resp = client.post(
            "/api/v4/auth/login",
            json={
                "email": "inactive@example.com",
                "password": "inactivepw1",
            },
        )
        assert resp.status_code == 403


# ─── Authed endpoints: /me + /logout ────────────────────────────────────────


class TestAuthenticatedEndpoints:
    def test_me_with_valid_token_returns_user(self, client, captured_emails):
        signup = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "me@example.com",
                "full_name": "Me",
                "password": "mepw123456",
            },
        ).json()
        access = signup["access_token"]

        resp = client.get("/api/v4/auth/me", headers={"Authorization": f"Bearer {access}"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["email"] == "me@example.com"
        assert body["full_name"] == "Me"
        assert body["plan"] == "trial"
        assert body["email_verified"] is False

    def test_me_without_token_returns_401(self, client):
        resp = client.get("/api/v4/auth/me")
        assert resp.status_code == 401

    def test_me_with_garbage_token_returns_401(self, client):
        resp = client.get("/api/v4/auth/me", headers={"Authorization": "Bearer not-a-jwt"})
        assert resp.status_code == 401

    def test_logout_with_valid_token(self, client, captured_emails):
        access = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "out@example.com",
                "full_name": "O",
                "password": "outpw12345",
            },
        ).json()["access_token"]

        resp = client.post("/api/v4/auth/logout", headers={"Authorization": f"Bearer {access}"})
        assert resp.status_code == 200


# ─── Password reset flow ────────────────────────────────────────────────────


class TestForgotPassword:
    def test_known_email_sends_reset_token(self, client, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "f@example.com",
                "full_name": "F",
                "password": "origpw12345",
            },
        )
        captured_emails["reset"].clear()

        resp = client.post("/api/v4/auth/forgot-password", json={"email": "f@example.com"})
        assert resp.status_code == 200
        assert len(captured_emails["reset"]) == 1
        assert captured_emails["reset"][0][0] == "f@example.com"

    def test_unknown_email_returns_200_without_sending(self, client, captured_emails):
        # Security: don't reveal whether the email exists.
        resp = client.post("/api/v4/auth/forgot-password", json={"email": "nosuchuser@example.com"})
        assert resp.status_code == 200
        assert captured_emails["reset"] == []


class TestResetPassword:
    def test_valid_token_updates_password(self, client, db_session, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "r@example.com",
                "full_name": "R",
                "password": "originalpw1",
            },
        )
        captured_emails["reset"].clear()
        client.post("/api/v4/auth/forgot-password", json={"email": "r@example.com"})
        _, token = captured_emails["reset"][0]

        resp = client.post(
            "/api/v4/auth/reset-password", json={"token": token, "password": "newpassword12"}
        )
        assert resp.status_code == 200

        # Old password no longer works.
        bad = client.post(
            "/api/v4/auth/login",
            json={
                "email": "r@example.com",
                "password": "originalpw1",
            },
        )
        assert bad.status_code == 401

        # New password works.
        good = client.post(
            "/api/v4/auth/login",
            json={
                "email": "r@example.com",
                "password": "newpassword12",
            },
        )
        assert good.status_code == 200

    def test_invalid_token_returns_400(self, client):
        resp = client.post(
            "/api/v4/auth/reset-password", json={"token": "fake", "password": "newpw123456"}
        )
        assert resp.status_code == 400

    def test_expired_token_returns_400(self, client, db_session, captured_emails):
        from src.models import User

        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "exp@example.com",
                "full_name": "E",
                "password": "originalpw1",
            },
        )
        captured_emails["reset"].clear()
        client.post("/api/v4/auth/forgot-password", json={"email": "exp@example.com"})
        _, token = captured_emails["reset"][0]

        # Force the token to be expired.
        user = db_session.query(User).filter(User.email == "exp@example.com").one()
        user.reset_token_expires = datetime.utcnow() - timedelta(hours=1)
        db_session.commit()

        resp = client.post(
            "/api/v4/auth/reset-password", json={"token": token, "password": "newpw123456"}
        )
        assert resp.status_code == 400

    def test_token_is_single_use(self, client, captured_emails):
        client.post(
            "/api/v4/auth/signup",
            json={
                "email": "su@example.com",
                "full_name": "S",
                "password": "originalpw1",
            },
        )
        captured_emails["reset"].clear()
        client.post("/api/v4/auth/forgot-password", json={"email": "su@example.com"})
        _, token = captured_emails["reset"][0]

        first = client.post(
            "/api/v4/auth/reset-password", json={"token": token, "password": "newpw1234567"}
        )
        assert first.status_code == 200

        # Second use must fail.
        again = client.post(
            "/api/v4/auth/reset-password", json={"token": token, "password": "evennewer123"}
        )
        assert again.status_code == 400


# ─── Refresh token ──────────────────────────────────────────────────────────


class TestRefreshToken:
    def test_refresh_with_access_token_issues_new_pair(self, client, captured_emails):
        # The refresh endpoint currently goes through get_current_user, which
        # only accepts access tokens. That's a known wart — the canonical
        # pattern is to accept a refresh token here — but this locks current
        # behavior so a future fix has a baseline to compare against.
        signup = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "ref@example.com",
                "full_name": "R",
                "password": "refpw123456",
            },
        ).json()
        resp = client.post(
            "/api/v4/auth/refresh",
            headers={"Authorization": f"Bearer {signup['access_token']}"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["access_token"] and body["refresh_token"]

    def test_refresh_token_path_currently_rejected(self, client, captured_emails):
        # Documents the bug above: passing the refresh_token gets 401 because
        # get_current_user requires access-type. Update this test when the
        # fix lands.
        signup = client.post(
            "/api/v4/auth/signup",
            json={
                "email": "ref2@example.com",
                "full_name": "R",
                "password": "refpw123456",
            },
        ).json()
        resp = client.post(
            "/api/v4/auth/refresh",
            headers={"Authorization": f"Bearer {signup['refresh_token']}"},
        )
        assert resp.status_code == 401
