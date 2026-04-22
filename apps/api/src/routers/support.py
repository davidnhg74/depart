"""Support tickets and contact form endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from datetime import datetime
import uuid

from ..db import get_db
from ..models import User, SupportTicket, TicketMessage
from ..auth.dependencies import get_current_user
from ..services.email import send_contact_notification, send_ticket_notification

router = APIRouter(prefix="/api/v4", tags=["support"])


# Pydantic models
class ContactFormRequest(BaseModel):
    name: str
    email: EmailStr
    subject: str
    message: str


class TicketCreateRequest(BaseModel):
    subject: str
    message: str  # First message body


class TicketMessageRequest(BaseModel):
    body: str


class TicketMessageResponse(BaseModel):
    id: str
    author_name: str
    is_staff: bool
    body: str
    created_at: str

    class Config:
        from_attributes = True


class TicketResponse(BaseModel):
    id: str
    subject: str
    status: str
    priority: str
    requester_email: str
    created_at: str
    updated_at: str
    message_count: int = 0

    class Config:
        from_attributes = True


class TicketDetailResponse(BaseModel):
    id: str
    subject: str
    status: str
    priority: str
    requester_email: str
    created_at: str
    updated_at: str
    messages: list[TicketMessageResponse]

    class Config:
        from_attributes = True


@router.post("/contact", status_code=status.HTTP_201_CREATED)
async def contact_form(
    request: ContactFormRequest,
):
    """Submit a contact form (no authentication required)."""
    # Send email to support team
    success = send_contact_notification(
        name=request.name,
        email=request.email,
        subject=request.subject,
        message=request.message,
    )

    if not success:
        # Still return 201 even if email failed (graceful degradation)
        pass

    return {
        "message": "Thank you for contacting us. We'll get back to you soon.",
        "email": request.email,
    }


@router.post("/support/tickets", response_model=TicketResponse, status_code=status.HTTP_201_CREATED)
async def create_ticket(
    request: TicketCreateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a new support ticket."""
    # Create ticket
    ticket = SupportTicket(
        id=uuid.uuid4(),
        user_id=current_user.id,
        subject=request.subject,
        status="open",
        requester_email=current_user.email,
    )

    # Add first message
    message = TicketMessage(
        id=uuid.uuid4(),
        ticket_id=ticket.id,
        author_id=current_user.id,
        is_staff=False,
        body=request.message,
    )

    db.add(ticket)
    db.add(message)
    db.commit()
    db.refresh(ticket)

    # Send notification email to support team
    send_ticket_notification(ticket.subject, ticket.requester_email)

    return TicketResponse(
        id=str(ticket.id),
        subject=ticket.subject,
        status=ticket.status,
        priority=ticket.priority,
        requester_email=ticket.requester_email,
        created_at=ticket.created_at.isoformat(),
        updated_at=ticket.updated_at.isoformat(),
        message_count=1,
    )


@router.get("/support/tickets", response_model=list[TicketResponse])
async def list_tickets(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List user's support tickets."""
    tickets = db.query(SupportTicket).filter(
        SupportTicket.user_id == current_user.id
    ).order_by(SupportTicket.created_at.desc()).all()

    return [
        TicketResponse(
            id=str(ticket.id),
            subject=ticket.subject,
            status=ticket.status,
            priority=ticket.priority,
            requester_email=ticket.requester_email,
            created_at=ticket.created_at.isoformat(),
            updated_at=ticket.updated_at.isoformat(),
            message_count=len(ticket.messages),
        )
        for ticket in tickets
    ]


@router.get("/support/tickets/{ticket_id}", response_model=TicketDetailResponse)
async def get_ticket(
    ticket_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get ticket details with all messages."""
    ticket = db.query(SupportTicket).filter(
        SupportTicket.id == ticket_id,
        SupportTicket.user_id == current_user.id,
    ).first()

    if not ticket:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ticket not found",
        )

    messages = [
        TicketMessageResponse(
            id=str(msg.id),
            author_name=msg.author.full_name or msg.author.email if msg.author else "Support Team",
            is_staff=msg.is_staff,
            body=msg.body,
            created_at=msg.created_at.isoformat(),
        )
        for msg in ticket.messages
    ]

    return TicketDetailResponse(
        id=str(ticket.id),
        subject=ticket.subject,
        status=ticket.status,
        priority=ticket.priority,
        requester_email=ticket.requester_email,
        created_at=ticket.created_at.isoformat(),
        updated_at=ticket.updated_at.isoformat(),
        messages=messages,
    )


@router.post("/support/tickets/{ticket_id}/messages")
async def add_ticket_message(
    ticket_id: str,
    request: TicketMessageRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Add a reply to a support ticket."""
    ticket = db.query(SupportTicket).filter(
        SupportTicket.id == ticket_id,
        SupportTicket.user_id == current_user.id,
    ).first()

    if not ticket:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ticket not found",
        )

    message = TicketMessage(
        id=uuid.uuid4(),
        ticket_id=ticket.id,
        author_id=current_user.id,
        is_staff=False,
        body=request.body,
    )

    ticket.updated_at = datetime.utcnow()
    db.add(message)
    db.commit()
    db.refresh(message)

    return {
        "id": str(message.id),
        "created_at": message.created_at.isoformat(),
    }


@router.put("/support/tickets/{ticket_id}/status")
async def update_ticket_status(
    ticket_id: str,
    status_update: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update ticket status (user can only close their own tickets)."""
    ticket = db.query(SupportTicket).filter(
        SupportTicket.id == ticket_id,
        SupportTicket.user_id == current_user.id,
    ).first()

    if not ticket:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ticket not found",
        )

    new_status = status_update.get("status", ticket.status)

    # Users can only close their own tickets
    if new_status not in ["open", "closed"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid status. Users can only set: open, closed",
        )

    ticket.status = new_status
    ticket.updated_at = datetime.utcnow()
    db.commit()

    return {"status": new_status}
