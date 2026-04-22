/**
 * Shared API client.
 *
 * Centralizes the axios instance, base URL handling, and the few
 * cross-cutting helpers (`logout`) that components import directly.
 *
 * Pages that POST multipart payloads (analyze, app-impact, runbook)
 * import `apiBaseUrl()` and build their own FormData; bodyless GETs
 * use the shared `api` instance.
 */
import axios, { AxiosInstance } from 'axios';
import Cookies from 'js-cookie';

import { useAuthStore } from '@/app/store/authStore';

export function apiBaseUrl(): string {
  // Server-rendered pages get the env var at build time; client pages
  // get it via Next's NEXT_PUBLIC_ inlining at the same point.
  return process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
}

function makeClient(): AxiosInstance {
  const client = axios.create({
    baseURL: apiBaseUrl(),
    withCredentials: true,
  });

  client.interceptors.request.use((config) => {
    const token = Cookies.get('access_token');
    if (token) {
      config.headers = config.headers || {};
      config.headers['Authorization'] = `Bearer ${token}`;
    }
    return config;
  });

  return client;
}

export const api = makeClient();

/** Sign out: clears server-side session, drops cookies + auth store. */
export async function logout(): Promise<void> {
  try {
    await api.post('/api/v4/auth/logout');
  } catch {
    // Logout is fire-and-forget; clearing local state below is what matters.
  }
  Cookies.remove('access_token');
  Cookies.remove('refresh_token');
  useAuthStore.getState().logout();
}

// ─── Auth helpers ────────────────────────────────────────────────────────────
//
// Thin wrappers around the auth router endpoints (src/routers/auth.py) +
// account router endpoints. Each returns the parsed response data and
// updates the local auth store + cookies as a side effect where the
// page UX expects it.

import type { User } from '@/app/store/authStore';

interface AuthTokens {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

interface LoginResponse extends AuthTokens {
  user: User;
}

function persistTokens(tokens: AuthTokens): void {
  Cookies.set('access_token', tokens.access_token, { sameSite: 'lax' });
  Cookies.set('refresh_token', tokens.refresh_token, { sameSite: 'lax' });
}

export async function login(email: string, password: string): Promise<LoginResponse> {
  const { data } = await api.post<LoginResponse>('/api/v4/auth/login', {
    email, password,
  });
  persistTokens(data);
  useAuthStore.getState().setUser(data.user);
  return data;
}

export async function signup(
  email: string, fullName: string, password: string,
): Promise<LoginResponse> {
  const { data } = await api.post<LoginResponse>('/api/v4/auth/signup', {
    email, full_name: fullName, password,
  });
  persistTokens(data);
  useAuthStore.getState().setUser(data.user);
  return data;
}

export async function forgotPassword(email: string): Promise<void> {
  await api.post('/api/v4/auth/forgot-password', { email });
}

export async function resetPassword(token: string, password: string): Promise<void> {
  await api.post('/api/v4/auth/reset-password', { token, password });
}

export async function verifyEmail(token: string): Promise<void> {
  await api.post('/api/v4/auth/verify-email', { token });
}

export async function fetchCurrentUser(): Promise<User | null> {
  try {
    const { data } = await api.get<User>('/api/v4/auth/me');
    useAuthStore.getState().setUser(data);
    return data;
  } catch {
    useAuthStore.getState().logout();
    return null;
  }
}
