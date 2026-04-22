"""Billing endpoints: Stripe checkout, portal, invoices, webhooks, and plan info."""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
import logging

from ..db import get_db
from ..models import User
from ..auth.dependencies import get_current_user
from ..config import settings
from ..services.billing import (
    get_plan_limits,
    create_checkout_session,
    create_portal_session,
    get_invoices,
    verify_webhook_signature,
    handle_subscription_created,
    handle_subscription_updated,
    handle_subscription_deleted,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v4/billing", tags=["billing"])


# Pydantic models
class PlanInfo(BaseModel):
    name: str
    price_monthly: Optional[int]  # in cents, None = custom/contact sales
    databases: Optional[int]
    migrations_per_month: Optional[int]
    llm_conversions: Optional[int]
    support: str


class CheckoutSessionRequest(BaseModel):
    plan: str  # starter, professional, enterprise


class CheckoutSessionResponse(BaseModel):
    checkout_url: str


class PortalSessionResponse(BaseModel):
    portal_url: str


class InvoiceItem(BaseModel):
    id: str
    amount: float
    currency: str
    created: str
    status: str
    paid: bool
    pdf_url: Optional[str] = None


class UsageResponse(BaseModel):
    plan: str
    databases_used: int
    databases_limit: Optional[int]
    migrations_used: int
    migrations_limit: Optional[int]
    llm_conversions_used: int
    llm_conversions_limit: Optional[int]


@router.get("/plans", response_model=List[PlanInfo])
async def list_plans():
    """Get all available subscription plans."""
    plans = [
        PlanInfo(
            name="trial",
            price_monthly=None,
            databases=1,
            migrations_per_month=3,
            llm_conversions=10,
            support="Community",
        ),
        PlanInfo(
            name="starter",
            price_monthly=24900,  # $249/month in cents
            databases=5,
            migrations_per_month=25,
            llm_conversions=100,
            support="Email",
        ),
        PlanInfo(
            name="professional",
            price_monthly=59900,  # $599/month in cents
            databases=20,
            migrations_per_month=100,
            llm_conversions=500,
            support="Priority Email + Slack",
        ),
        PlanInfo(
            name="enterprise",
            price_monthly=None,  # Custom pricing
            databases=None,
            migrations_per_month=None,
            llm_conversions=None,
            support="Dedicated CSM + SLA",
        ),
    ]
    return plans


@router.post("/checkout", response_model=CheckoutSessionResponse)
async def create_checkout(
    request: CheckoutSessionRequest,
    current_user: User = Depends(get_current_user),
):
    """Create a Stripe Checkout session to upgrade plan."""
    if request.plan not in ["starter", "professional", "enterprise"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid plan. Must be: starter, professional, or enterprise",
        )

    if request.plan == "enterprise":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Enterprise plan requires contacting sales. Email support@depart.io",
        )

    # Build success/cancel URLs
    frontend_url = settings.frontend_url
    success_url = f"{frontend_url}/billing?success=true&plan={request.plan}"
    cancel_url = f"{frontend_url}/billing?canceled=true"

    checkout_url = create_checkout_session(current_user, request.plan, success_url, cancel_url)

    if not checkout_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing service temporarily unavailable. Try again later.",
        )

    return CheckoutSessionResponse(checkout_url=checkout_url)


@router.get("/portal", response_model=PortalSessionResponse)
async def get_portal_session(
    current_user: User = Depends(get_current_user),
):
    """Get Stripe Customer Portal session URL for managing subscription."""
    if not current_user.stripe_customer_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No active subscription. Create one first via /checkout",
        )

    frontend_url = settings.frontend_url
    return_url = f"{frontend_url}/billing"

    portal_url = create_portal_session(current_user, return_url)

    if not portal_url:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Billing service temporarily unavailable. Try again later.",
        )

    return PortalSessionResponse(portal_url=portal_url)


@router.get("/invoices", response_model=List[InvoiceItem])
async def list_invoices(
    current_user: User = Depends(get_current_user),
):
    """Get user's invoice history from Stripe."""
    invoices = get_invoices(current_user, limit=20)
    return [InvoiceItem(**inv) for inv in invoices]


@router.get("/usage", response_model=UsageResponse)
async def get_usage(
    current_user: User = Depends(get_current_user),
):
    """Get current month usage vs plan limits."""
    limits = get_plan_limits(current_user.plan.value)
    return UsageResponse(
        plan=current_user.plan.value,
        databases_used=current_user.databases_used,
        databases_limit=limits.get("databases"),
        migrations_used=current_user.migrations_used_this_month,
        migrations_limit=limits.get("migrations_per_month"),
        llm_conversions_used=current_user.llm_conversions_this_month,
        llm_conversions_limit=limits.get("llm_per_month"),
    )


@router.post("/webhook", status_code=status.HTTP_200_OK)
async def webhook(
    request: Request,
    db: Session = Depends(get_db),
):
    """Handle Stripe webhook events (subscription created/updated/deleted)."""
    body = await request.body()
    signature = request.headers.get("stripe-signature")

    if not signature:
        logger.warning("Webhook received without stripe-signature header")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing signature")

    # Verify webhook signature
    event = verify_webhook_signature(body, signature)
    if not event:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid signature")

    event_type = event.get("type")
    logger.info(f"Processing Stripe webhook: {event_type}")

    # Handle different event types
    handled = False

    if event_type == "customer.subscription.created":
        handled = handle_subscription_created(event, db)
    elif event_type == "customer.subscription.updated":
        handled = handle_subscription_updated(event, db)
    elif event_type == "customer.subscription.deleted":
        handled = handle_subscription_deleted(event, db)
    elif event_type in ["invoice.payment_succeeded", "invoice.payment_failed"]:
        # Could implement invoice logging/alerting here
        logger.info(f"Invoice event: {event_type}")
        handled = True
    else:
        logger.debug(f"Unhandled webhook event: {event_type}")
        handled = True  # Still return 200 to acknowledge

    if not handled:
        logger.error(f"Failed to handle webhook: {event_type}")
        return {"status": "error"}

    return {"status": "success", "event_type": event_type}
