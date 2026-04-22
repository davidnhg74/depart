import '@testing-library/jest-dom/vitest';

// Pin a deterministic API base URL so axios calls in components are stable.
process.env.NEXT_PUBLIC_API_URL = 'http://test-api';
