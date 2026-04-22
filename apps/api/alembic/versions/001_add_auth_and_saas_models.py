"""Add auth and SaaS models

Revision ID: 001
Revises:
Create Date: 2026-04-21 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = '000_baseline'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Define each enum once with create_type=False, then explicitly create
    # them up front. Reusing the same instance in column definitions below
    # ensures op.create_table doesn't re-emit CREATE TYPE for each table
    # that uses the type (which is what the previous sa.Enum-per-column
    # pattern was silently doing — sa.Enum's create_type=False flag does
    # not always propagate through the dialect-specific DDL compiler).
    plan_enum = postgresql.ENUM(
        "trial", "starter", "professional", "enterprise",
        name="plan_enum", create_type=False,
    )
    ticket_status_enum = postgresql.ENUM(
        "open", "in_progress", "resolved", "closed",
        name="ticket_status_enum", create_type=False,
    )
    ticket_priority_enum = postgresql.ENUM(
        "low", "medium", "high", "critical",
        name="ticket_priority_enum", create_type=False,
    )

    bind = op.get_bind()
    plan_enum.create(bind, checkfirst=True)
    ticket_status_enum.create(bind, checkfirst=True)
    ticket_priority_enum.create(bind, checkfirst=True)

    # Create users table
    op.create_table(
        'users',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column('email', sa.String(255), nullable=False, unique=True, index=True),
        sa.Column('full_name', sa.String(255), nullable=True),
        sa.Column('hashed_password', sa.String(255), nullable=False),
        sa.Column('email_verified', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('email_verify_token', sa.String(255), nullable=True),
        sa.Column('reset_token', sa.String(255), nullable=True),
        sa.Column('reset_token_expires', sa.DateTime(timezone=True), nullable=True),
        sa.Column('plan', plan_enum, nullable=False, server_default='trial'),
        sa.Column('stripe_customer_id', sa.String(255), nullable=True),
        sa.Column('stripe_subscription_id', sa.String(255), nullable=True),
        sa.Column('subscription_status', sa.String(50), nullable=True),
        sa.Column('trial_starts_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('trial_expires_at', sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now() + interval '14 days'")),
        sa.Column('databases_used', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('migrations_used_this_month', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('llm_conversions_this_month', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('usage_reset_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )

    # Create api_keys table
    op.create_table(
        'api_keys',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('key_hash', sa.String(255), nullable=False, unique=True),
        sa.Column('key_prefix', sa.String(8), nullable=False),
        sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )

    # Create subscriptions table
    op.create_table(
        'subscriptions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('stripe_subscription_id', sa.String(255), nullable=False, unique=True),
        sa.Column('plan', plan_enum, nullable=False),
        sa.Column('status', sa.String(50), nullable=False),
        sa.Column('current_period_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('current_period_end', sa.DateTime(timezone=True), nullable=False),
        sa.Column('canceled_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id')
    )

    # Create support_tickets table
    op.create_table(
        'support_tickets',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('subject', sa.String(255), nullable=False),
        sa.Column('status', ticket_status_enum, nullable=False, server_default='open'),
        sa.Column('priority', ticket_priority_enum, nullable=False, server_default='medium'),
        sa.Column('requester_email', sa.String(255), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id')
    )

    # Create ticket_messages table
    op.create_table(
        'ticket_messages',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, server_default=sa.text("gen_random_uuid()")),
        sa.Column('ticket_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('author_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('is_staff', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('body', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(['ticket_id'], ['support_tickets.id']),
        sa.ForeignKeyConstraint(['author_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id')
    )

    # Add user_id column to analysis_jobs table
    op.add_column('analysis_jobs', sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.create_foreign_key('fk_analysis_jobs_user_id', 'analysis_jobs', 'users', ['user_id'], ['id'])


def downgrade() -> None:
    # Drop foreign key from analysis_jobs
    op.drop_constraint('fk_analysis_jobs_user_id', 'analysis_jobs')
    op.drop_column('analysis_jobs', 'user_id')

    # Drop tables
    op.drop_table('ticket_messages')
    op.drop_table('support_tickets')
    op.drop_table('subscriptions')
    op.drop_table('api_keys')
    op.drop_table('users')

    # Drop enum types
    op.execute("DROP TYPE IF EXISTS ticket_priority_enum")
    op.execute("DROP TYPE IF EXISTS ticket_status_enum")
    op.execute("DROP TYPE IF EXISTS plan_enum")
