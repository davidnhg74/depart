from sqlalchemy import Column, String, Integer, DateTime, JSON, ForeignKey, Text, Enum, ARRAY, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid
import enum
from .db import Base


class JobStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"


class Lead(Base):
    __tablename__ = "leads"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    jobs = relationship("AnalysisJob", back_populates="lead")


class AnalysisJob(Base):
    __tablename__ = "analysis_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lead_id = Column(UUID(as_uuid=True), ForeignKey("leads.id"), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, nullable=False)
    complexity_report = Column(JSON, nullable=True)
    pdf_path = Column(String(512), nullable=True)
    rate_per_day = Column(Integer, default=1000, nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)

    lead = relationship("Lead", back_populates="jobs")


class ConversionCaseRecord(Base):
    """RAG conversion case storage for pattern learning."""

    __tablename__ = "conversion_cases"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    construct_type = Column(String(50), nullable=False, index=True)  # PROCEDURE, FUNCTION, etc.
    oracle_code = Column(Text, nullable=False)
    postgres_code = Column(Text, nullable=False)
    embedding = Column(ARRAY(Float), nullable=False)  # Vector embedding
    success_count = Column(Integer, default=1, nullable=False)
    fail_count = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.fail_count
        return self.success_count / total if total > 0 else 0.0
