"""Email service using Resend."""

import logging
from ..config import settings

logger = logging.getLogger(__name__)

# Try to import resend, but make it optional
try:
    import resend

    resend.api_key = settings.resend_api_key
    RESEND_AVAILABLE = bool(settings.resend_api_key)
except ImportError:
    RESEND_AVAILABLE = False


def send_verification_email(email: str, token: str, frontend_url: str) -> bool:
    """Send email verification link."""
    if not RESEND_AVAILABLE:
        logger.warning(f"Resend not configured; skipping verification email to {email}")
        return False

    verify_url = f"{frontend_url}/verify-email?token={token}"
    html_content = f"""
    <h1>Verify Your Email</h1>
    <p>Click the link below to verify your email address:</p>
    <p><a href="{verify_url}">Verify Email</a></p>
    <p>Or copy this link: {verify_url}</p>
    <p>This link expires in 24 hours.</p>
    """

    try:
        resend.Emails.send(
            {
                "from": "Depart <noreply@depart.io>",
                "to": email,
                "subject": "Verify Your Depart Account",
                "html": html_content,
            }
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send verification email to {email}: {e}")
        return False


def send_password_reset_email(email: str, token: str, frontend_url: str) -> bool:
    """Send password reset link."""
    if not RESEND_AVAILABLE:
        logger.warning(f"Resend not configured; skipping reset email to {email}")
        return False

    reset_url = f"{frontend_url}/reset-password?token={token}"
    html_content = f"""
    <h1>Reset Your Password</h1>
    <p>Click the link below to reset your password:</p>
    <p><a href="{reset_url}">Reset Password</a></p>
    <p>Or copy this link: {reset_url}</p>
    <p>This link expires in 1 hour.</p>
    <p>If you didn't request this, please ignore this email.</p>
    """

    try:
        resend.Emails.send(
            {
                "from": "Depart <noreply@depart.io>",
                "to": email,
                "subject": "Reset Your Depart Password",
                "html": html_content,
            }
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send reset email to {email}: {e}")
        return False


def send_contact_notification(name: str, email: str, subject: str, message: str) -> bool:
    """Send contact form submission to support team."""
    if not RESEND_AVAILABLE:
        logger.warning("Resend not configured; skipping contact notification")
        return False

    html_content = f"""
    <h1>New Contact Form Submission</h1>
    <p><strong>Name:</strong> {name}</p>
    <p><strong>Email:</strong> {email}</p>
    <p><strong>Subject:</strong> {subject}</p>
    <p><strong>Message:</strong></p>
    <p>{message.replace(chr(10), '<br>')}</p>
    """

    try:
        resend.Emails.send(
            {
                "from": "Depart <noreply@depart.io>",
                "to": settings.support_email,
                "subject": f"Contact Form: {subject}",
                "html": html_content,
                "reply_to": email,
            }
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send contact notification: {e}")
        return False


def send_ticket_notification(ticket_subject: str, requester_email: str) -> bool:
    """Notify support team of new ticket."""
    if not RESEND_AVAILABLE:
        logger.warning("Resend not configured; skipping ticket notification")
        return False

    html_content = f"""
    <h1>New Support Ticket</h1>
    <p><strong>Subject:</strong> {ticket_subject}</p>
    <p><strong>From:</strong> {requester_email}</p>
    <p><a href="https://depart.io/admin/tickets">View in Dashboard</a></p>
    """

    try:
        resend.Emails.send(
            {
                "from": "Depart <noreply@depart.io>",
                "to": settings.support_email,
                "subject": f"New Support Ticket: {ticket_subject}",
                "html": html_content,
            }
        )
        return True
    except Exception as e:
        logger.error(f"Failed to send ticket notification: {e}")
        return False
