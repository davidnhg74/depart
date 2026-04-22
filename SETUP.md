# Depart Setup Guide

## Prerequisites

- PostgreSQL 13+ running locally
- Python 3.9+
- Node.js 18+
- npm or yarn

## Quick Start

### 1. Set up PostgreSQL

Make sure PostgreSQL is running:

```bash
# Start PostgreSQL (macOS with Homebrew)
pg_ctl -D /usr/local/var/postgres start

# Or check if it's already running
psql -U postgres -c "SELECT version();"
```

### 2. Initialize the Database

```bash
# From the project root
./setup_db.sh
```

This script will:
- Drop and recreate the `depart_dev` database
- Create all required tables and extensions
- Set up indexes

Credentials used:
- Host: `localhost`
- Port: `5432`
- User: `depart`
- Password: `depart_dev_pw`
- Database: `depart_dev`

### 3. Start the Backend API

```bash
cd apps/api

# Install dependencies (first time only)
pip install -r requirements.txt

# Start the server
python3 -m uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

Health check: `curl http://localhost:8000/health`

### 4. Start the Frontend

In a new terminal:

```bash
cd apps/web

# Install dependencies (first time only)
npm install

# Start development server
npm run dev
```

The frontend will be available at: `http://localhost:3000`

### 5. Create Your Account

1. Open `http://localhost:3000`
2. Click "Sign up"
3. Fill in your email, name, and password
4. Click "Sign up" to create your account

**Note:** Email verification emails won't be sent in development unless you configure Resend API key. The signup will succeed but email verification can be skipped.

## Environment Variables

### Backend (`apps/api/.env`)

```env
# Database
DATABASE_URL=postgresql+asyncpg://depart:depart_dev_pw@localhost:5432/depart_dev

# API
ENVIRONMENT=development
FRONTEND_URL=http://localhost:3000

# Optional: AI/LLM (for conversion features)
ANTHROPIC_API_KEY=sk-...

# Optional: Email (Resend)
RESEND_API_KEY=re_...
SUPPORT_EMAIL=support@depart.io

# Optional: Stripe (billing)
STRIPE_SECRET_KEY=sk_test_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRICE_STARTER=price_...
STRIPE_PRICE_PROFESSIONAL=price_...
STRIPE_PRICE_ENTERPRISE=price_...

# JWT
JWT_SECRET_KEY=your-super-secret-key-change-in-production
```

### Frontend (`apps/web/.env.local`)

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

## Troubleshooting

### Database Connection Error

```
Error: could not connect to server: Connection refused
```

**Solution:** Start PostgreSQL:
```bash
pg_ctl -D /usr/local/var/postgres start
```

### Backend Port Already in Use

```
Address already in use (:8000)
```

**Solution:** Kill the existing process:
```bash
lsof -ti:8000 | xargs kill -9
```

### Signup Returns 500 Error

Check backend logs for detailed error. Common causes:
1. Database is not running (see Database Connection Error)
2. Tables don't exist (run `./setup_db.sh`)
3. Backend needs to be restarted after database changes

### CORS Errors in Browser Console

```
Access to XMLHttpRequest blocked by CORS policy
```

**Solution:** This usually means:
1. Backend is not running (check `http://localhost:8000/health`)
2. Frontend URL in backend config doesn't match your actual frontend URL
3. Restart both frontend and backend

## Database Schema

The database includes tables for:
- **users** - User accounts with subscription info
- **analysis_jobs** - Schema analysis jobs
- **api_keys** - API keys for programmatic access
- **subscriptions** - Stripe subscription records
- **support_tickets** - Customer support tickets
- **ticket_messages** - Support ticket messages

All sensitive data (database passwords, API keys) is never logged or stored in database.

## Testing Features

### Analyzer
1. Go to http://localhost:3000/analyzer
2. Upload a ZIP file with Oracle SQL/PL-SQL code
3. Get complexity analysis and PDF report

### Converter
1. Go to http://localhost:3000/converter
2. Paste Oracle PL/SQL code
3. Get PostgreSQL equivalent

### Dashboard
1. Sign up and verify email
2. Go to http://localhost:3000/dashboard
3. See usage metrics and recent jobs

### Settings
1. Go to http://localhost:3000/settings
2. Change profile, security settings
3. Generate API keys

### Billing
1. Go to http://localhost:3000/billing
2. See current plan and usage
3. (Requires Stripe keys configured)

## Development Tips

- Frontend hot reloads on file changes
- Backend auto-reloads with `--reload` flag
- Check database with: `psql -U depart depart_dev`
- View API docs at: http://localhost:8000/docs
- API logs show SQL when `ENVIRONMENT=development`
