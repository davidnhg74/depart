import { describe, it, expect, beforeEach, afterEach } from 'vitest';

import { apiBaseUrl } from './api';

describe('apiBaseUrl', () => {
  const original = process.env.NEXT_PUBLIC_API_URL;

  afterEach(() => {
    process.env.NEXT_PUBLIC_API_URL = original;
  });

  it('returns the configured URL when set', () => {
    process.env.NEXT_PUBLIC_API_URL = 'https://api.example.com';
    expect(apiBaseUrl()).toBe('https://api.example.com');
  });

  it('falls back to localhost when unset', () => {
    delete process.env.NEXT_PUBLIC_API_URL;
    expect(apiBaseUrl()).toBe('http://localhost:8000');
  });
});
