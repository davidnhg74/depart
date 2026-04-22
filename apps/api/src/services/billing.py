"""Stripe billing service - handle subscriptions, checkout, webhooks."""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from ..config import settings
from ..models import User, PlanEnum, Subscription
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Try to import stripe
try:
    import stripe

    stripe.api_key = settings.stripe_secret_key
    STRIPE_AVAILABLE = bool(settings.stripe_secret_key)
except ImportError:
    STRIPE_AVAILABLE = False
    logger.warning("Stripe SDK not installed; billing features disabled")


PLAN_LIMITS = {
    "trial": {"databases": 1, "migrations_per_month": 3, "llm_per_month": 10, "days": 14},
    "starter": {"databases": 5, "migrations_per_month": 25, "llm_per_month": 100},
    "professional": {"databases": 20, "migrations_per_month": 100, "llm_per_month": 500},
    "enterprise": {"databases": None, "migrations_per_month": None, "llm_per_month": None},
}


def get_plan_limits(plan: str) -> dict:
    """Get usage limits for a plan tier."""
    return PLAN_LIMITS.get(plan, PLAN_LIMITS["trial"])


def create_checkout_session(
    user: User, plan: str, success_url: str, cancel_url: str
) -> Optional[str]:
    """Create a Stripe Checkout session for plan upgrade."""
    if not STRIPE_AVAILABLE:
        logger.warning("Stripe not configured; cannot create checkout session")
        return None

    if plan not in settings.stripe_price_ids:
        logger.error(f"Unknown plan: {plan}")
        return None

    price_id = settings.stripe_price_ids[plan]
    if not price_id:
        logger.error(f"No Stripe price ID configured for plan: {plan}")
        return None

    try:
        # Create or get Stripe customer
        if not user.stripe_customer_id:
            customer = stripe.Customer.create(
                email=user.email,
                name=user.full_name or user.email,
                metadata={"user_id": str(user.id)},
            )
            user.stripe_customer_id = customer.id
        else:
            customer = stripe.Customer.retrieve(user.stripe_customer_id)

        # Create checkout session
        session = stripe.checkout.Session.create(
            customer=user.stripe_customer_id,
            line_items=[
                {
                    "price": price_id,
                    "quantity": 1,
                }
            ],
            mode="subscription",
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={"plan": plan, "user_id": str(user.id)},
        )

        return session.url

    except stripe.error.StripeError as e:
        logger.error(f"Stripe checkout error: {e}")
        return None


def create_portal_session(user: User, return_url: str) -> Optional[str]:
    """Create a Stripe Customer Portal session for subscription management."""
    if not STRIPE_AVAILABLE:
        logger.warning("Stripe not configured; cannot create portal session")
        return None

    if not user.stripe_customer_id:
        logger.error(f"User {user.id} has no Stripe customer ID")
        return None

    try:
        session = stripe.billing.portal.Session.create(
            customer=user.stripe_customer_id,
            return_url=return_url,
        )
        return session.url

    except stripe.error.StripeError as e:
        logger.error(f"Stripe portal error: {e}")
        return None


def get_invoices(user: User, limit: int = 10) -> list:
    """Get user's invoices from Stripe."""
    if not STRIPE_AVAILABLE or not user.stripe_customer_id:
        return []

    try:
        invoices = stripe.Invoice.list(customer=user.stripe_customer_id, limit=limit)
        return [
            {
                "id": inv.id,
                "amount": inv.amount_paid / 100,  # Convert cents to dollars
                "currency": inv.currency.upper(),
                "created": datetime.fromtimestamp(inv.created).isoformat(),
                "status": inv.status,
                "paid": inv.paid,
                "pdf_url": inv.invoice_pdf,
            }
            for inv in invoices.data
        ]
    except stripe.error.StripeError as e:
        logger.error(f"Error fetching invoices: {e}")
        return []


def handle_subscription_created(event: Dict[str, Any], db: Session) -> bool:
    """Handle customer.subscription.created webhook event."""
    try:
        subscription = event["data"]["object"]
        user_id = subscription.get("metadata", {}).get("user_id")

        if not user_id:
            logger.warning("Subscription created without user_id metadata")
            return False

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            logger.error(f"User {user_id} not found for subscription {subscription['id']}")
            return False

        # Extract plan from subscription
        items = subscription.get("items", {}).get("data", [])
        plan = None
        if items:
            plan_id = items[0].get("price", {}).get("id")
            for plan_name, stripe_id in settings.stripe_price_ids.items():
                if stripe_id == plan_id:
                    plan = plan_name
                    break

        if not plan:
            logger.warning(f"Could not determine plan from subscription {subscription['id']}")
            plan = "starter"  # Default

        # Update user
        user.stripe_subscription_id = subscription["id"]
        user.subscription_status = subscription["status"]
        user.plan = PlanEnum(plan)
        user.stripe_customer_id = subscription["customer"]

        # Log subscription event
        sub_record = Subscription(
            user_id=user.id,
            stripe_subscription_id=subscription["id"],
            plan=plan,
            status=subscription["status"],
            current_period_start=datetime.fromtimestamp(subscription["current_period_start"]),
            current_period_end=datetime.fromtimestamp(subscription["current_period_end"]),
        )
        db.add(sub_record)
        db.commit()

        logger.info(f"Subscription created for user {user_id}: {subscription['id']} ({plan})")
        return True

    except Exception as e:
        logger.error(f"Error handling subscription created: {e}")
        return False


def handle_subscription_updated(event: Dict[str, Any], db: Session) -> bool:
    """Handle customer.subscription.updated webhook event."""
    try:
        subscription = event["data"]["object"]
        user = db.query(User).filter(User.stripe_subscription_id == subscription["id"]).first()

        if not user:
            logger.warning(f"User not found for subscription {subscription['id']}")
            return False

        user.subscription_status = subscription["status"]

        # Update subscription record
        sub_record = (
            db.query(Subscription)
            .filter(Subscription.stripe_subscription_id == subscription["id"])
            .first()
        )
        if sub_record:
            sub_record.status = subscription["status"]
            sub_record.current_period_end = datetime.fromtimestamp(
                subscription["current_period_end"]
            )
            sub_record.updated_at = datetime.utcnow()

        db.commit()

        logger.info(f"Subscription updated for user {user.id}: {subscription['id']}")
        return True

    except Exception as e:
        logger.error(f"Error handling subscription updated: {e}")
        return False


def handle_subscription_deleted(event: Dict[str, Any], db: Session) -> bool:
    """Handle customer.subscription.deleted webhook event."""
    try:
        subscription = event["data"]["object"]
        user = db.query(User).filter(User.stripe_subscription_id == subscription["id"]).first()

        if not user:
            logger.warning(f"User not found for subscription {subscription['id']}")
            return False

        # Revert to trial plan
        user.plan = PlanEnum.TRIAL
        user.subscription_status = "canceled"
        user.stripe_subscription_id = None

        # Update subscription record
        sub_record = (
            db.query(Subscription)
            .filter(Subscription.stripe_subscription_id == subscription["id"])
            .first()
        )
        if sub_record:
            sub_record.status = "canceled"
            sub_record.canceled_at = datetime.utcnow()
            sub_record.updated_at = datetime.utcnow()

        db.commit()

        logger.info(f"Subscription deleted for user {user.id}: {subscription['id']}")
        return True

    except Exception as e:
        logger.error(f"Error handling subscription deleted: {e}")
        return False


def verify_webhook_signature(body: bytes, signature: str) -> Optional[Dict[str, Any]]:
    """Verify Stripe webhook signature and return parsed event."""
    if not STRIPE_AVAILABLE:
        logger.warning("Stripe not configured; cannot verify webhook signature")
        return None

    try:
        event = stripe.Webhook.construct_event(body, signature, settings.stripe_webhook_secret)
        return event
    except ValueError:
        logger.error("Invalid webhook payload")
        return None
    except stripe.error.SignatureVerificationError:
        logger.error("Invalid webhook signature")
        return None
