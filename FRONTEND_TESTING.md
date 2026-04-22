# Depart Frontend Testing Guide

## Prerequisites

- Node.js 18+
- npm or yarn
- API running on localhost:8000 (or configured in .env)

## Setup

```bash
cd apps/web
npm install
npm run dev
```

Open browser: http://localhost:3000

## Test Scenarios

### 1. Home Page (/)

**Expected:**
- Hero section with "Oracle to PostgreSQL Migration" title
- "Get Started" button
- Navigation menu visible
- Responsive design works on mobile

**Test:**
- [ ] Page loads without errors
- [ ] All elements render
- [ ] Links work
- [ ] Resize browser - layout adapts

### 2. Complexity Analysis Page (/analyze)

**Expected:**
- File upload form
- Rate/day input (default 1000)
- "Analyze" button
- Progress indicator after submit
- Results display with:
  - Total lines, auto-convertible, needs-review, must-rewrite counts
  - Effort estimate
  - Top 10 constructs
  - PDF download option

**Test:**
- [ ] Upload small SQL file (test_files/sample.sql)
- [ ] Click Analyze
- [ ] Progress shows
- [ ] Results appear
- [ ] Download PDF works
- [ ] Different file sizes handled correctly
- [ ] Error handling (invalid files, too large)

### 3. Conversion Page (/convert)

**Expected:**
- Left panel: construct type selector, buttons
- Right panel: code editor (Monaco) and templates
- After conversion: DiffViewer showing red/green diff
- Semantic issues panel below

**Test:**
- [ ] Select "Procedure" from dropdown
- [ ] Click template "Simple Proc"
- [ ] Code appears in editor
- [ ] Click "Convert"
- [ ] DiffViewer appears with Oracle on left, PostgreSQL on right
- [ ] Red/green line highlighting works
- [ ] Copy to clipboard works
- [ ] Download SQL works
- [ ] Switch to "Function" - templates change
- [ ] Error handling (empty code, invalid syntax)

### 4. Conversion Features

**Test Oracle Functions:**
- [ ] DECODE → CASE conversion visible in diff
- [ ] NVL → COALESCE conversion visible
- [ ] SYSDATE → CURRENT_DATE visible
- [ ] DBMS_OUTPUT.PUT_LINE → RAISE NOTICE

**Test Schema DDL:**
- [ ] NUMBER → NUMERIC
- [ ] VARCHAR2 → VARCHAR
- [ ] DATE → TIMESTAMP
- [ ] PRIMARY KEY preserved

### 5. Test Results Page (/test-results)

**How to reach:**
```
http://localhost:3000/test-results?migration_id=test-123
```

**Expected:**
- Summary cards (total objects, converted, tests generated, %)
- Progress bars showing conversion and test pass rate
- Risk heatmap with color-coded cells (red/amber/yellow/green)
- Blockers list (if any)
- Download pgTAP SQL button
- Back to Converter link

**Test:**
- [ ] Page loads with valid migration_id
- [ ] Cards display correct values
- [ ] Progress bars animate smoothly
- [ ] Heatmap cells render with correct colors
- [ ] Hover over heatmap cells shows tooltip
- [ ] Download button works
- [ ] Error message appears for invalid/missing migration_id

### 6. Risk Heatmap Component

**Expected:**
- Grid of colored boxes (red/amber/yellow-green/green)
- Hover shows name + construct_type + risk level
- Legend at bottom
- Responsive grid layout

**Test:**
- [ ] Risk colors display correctly:
  - Red = high risk
  - Amber = medium risk
  - Yellow = low risk
  - Green = safe
- [ ] Tooltips appear on hover
- [ ] Legend shows all 4 risk levels
- [ ] Grid responsive on mobile

### 7. DiffViewer Component

**Expected:**
- Monaco Editor diff view
- Left side: Original Oracle code
- Right side: Converted PostgreSQL code
- Line-level diff highlighting (red/green)
- Line numbers
- Syntax highlighting

**Test:**
- [ ] Diff appears after conversion
- [ ] Added lines highlighted in green
- [ ] Removed/changed lines in red
- [ ] Syntax highlighting works
- [ ] Scroll synchronization (both sides scroll together)
- [ ] Zoom works
- [ ] Copy button copies converted code

### 8. Navigation & Layout

**Expected:**
- Consistent header with logo + nav menu
- Footer with links
- Responsive navigation (mobile hamburger menu)
- Active page indicator in nav

**Test:**
- [ ] All nav links work
- [ ] Current page highlighted
- [ ] Mobile menu opens/closes
- [ ] Links go to correct pages
- [ ] No console errors

### 9. Error Handling

**Test error scenarios:**
- [ ] Empty file upload - error message appears
- [ ] 404 page - shows gracefully
- [ ] API down - shows connection error
- [ ] Timeout - shows retry option
- [ ] Invalid input - form validation works

### 10. Performance

**Test:**
- [ ] Initial load < 3 seconds
- [ ] Conversion completes in < 10 seconds
- [ ] Page transitions smooth
- [ ] No memory leaks (DevTools)
- [ ] Images optimized
- [ ] CSS/JS minified

## Browser Testing

Test on:
- [ ] Chrome/Chromium (latest)
- [ ] Firefox (latest)
- [ ] Safari (if macOS)
- [ ] Mobile Safari (if iOS)
- [ ] Mobile Chrome (if Android)

## Responsive Design

Test breakpoints:
- [ ] Mobile (320px, 375px, 414px)
- [ ] Tablet (768px)
- [ ] Desktop (1024px, 1440px, 1920px)

## Accessibility

- [ ] Tab navigation works
- [ ] Color contrast sufficient
- [ ] Alt text on images
- [ ] Form labels associated
- [ ] Error messages clear

## Local Testing Commands

```bash
# Development mode
npm run dev

# Build for production
npm run build

# Run production build locally
npm run start

# Type checking
npm run typecheck

# Linting
npm run lint
```

## Sample Test Files

Create `test_files/sample.sql`:

```sql
CREATE OR REPLACE PROCEDURE greet(p_name VARCHAR2) AS
BEGIN
  DBMS_OUTPUT.PUT_LINE('Hello ' || p_name);
END greet;
```

Create `test_files/schema.sql`:

```sql
CREATE TABLE employees (
    employee_id NUMBER(6) PRIMARY KEY,
    first_name VARCHAR2(50) NOT NULL,
    salary NUMBER(10,2),
    hire_date DATE
);
```

## Common Issues & Fixes

### API Connection Error
- Verify API running on localhost:8000
- Check NEXT_PUBLIC_API_URL in .env.local
- CORS headers in API

### Monaco Editor Not Loading
- Check @monaco-editor/react installed
- Build might be needed: npm run build

### DiffViewer Blank
- Verify Editor imported correctly
- Check converted code being passed
- Browser console errors?

### Tailwind Styles Not Working
- Rebuild CSS: npm run build
- Check tailwind.config.js
- Purge cache: rm -rf .next

## Sign-off Checklist

- [ ] All pages load without errors
- [ ] Conversion produces correct output
- [ ] DiffViewer works correctly
- [ ] Test results page displays properly
- [ ] Risk heatmap renders with correct colors
- [ ] Navigation works throughout app
- [ ] Responsive on mobile devices
- [ ] No console errors or warnings
- [ ] Performance acceptable
- [ ] Error handling graceful
- [ ] Accessibility baseline met

## Reporting Issues

When testing, note:
- Browser & version
- URL being tested
- Steps to reproduce
- Expected vs actual behavior
- Screenshots/video if helpful
- Console errors (DevTools > Console)
