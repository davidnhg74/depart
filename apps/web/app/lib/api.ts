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

// Lazy singleton — vi.mock('axios') in component tests stubs `axios.create`
// to undefined, so module-load-time construction would crash. The client is
// built on first access and cached after.
let _api: AxiosInstance | null = null;

function getClient(): AxiosInstance {
  if (_api === null) _api = makeClient();
  return _api;
}

export const api: AxiosInstance = new Proxy({} as AxiosInstance, {
  get(_target, prop) {
    return Reflect.get(getClient(), prop);
  },
});

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


// ─── Self-hosted auth (/api/v1/auth/*) ──────────────────────────────────────
//
// Parallel helpers for the self-hosted auth router. The endpoints return a
// narrower shape than the cloud equivalents (tokens only, no user — call
// /me to populate the store), and the session has no signup/reset flows.

export async function loginLocal(email: string, password: string): Promise<void> {
  const { data } = await api.post<AuthTokens>('/api/v1/auth/login', { email, password });
  persistTokens(data);
  // /me now sees the token through the axios interceptor.
  const { data: user } = await api.get<User>('/api/v1/auth/me');
  useAuthStore.getState().setUser(user);
}


export async function logoutLocal(): Promise<void> {
  try {
    await api.post('/api/v1/auth/logout');
  } catch {
    // noop — logout is fire-and-forget
  }
  Cookies.remove('access_token');
  Cookies.remove('refresh_token');
  useAuthStore.getState().logout();
}


export async function fetchCurrentUserLocal(): Promise<User | null> {
  try {
    const { data } = await api.get<User>('/api/v1/auth/me');
    useAuthStore.getState().setUser(data);
    return data;
  } catch {
    useAuthStore.getState().logout();
    return null;
  }
}


// ─── Bootstrap (first-run setup) ────────────────────────────────────────────

export interface SetupStatus {
  needs_bootstrap: boolean;
  admin_count: number;
}


export async function getSetupStatus(): Promise<SetupStatus> {
  const { data } = await api.get<SetupStatus>('/api/v1/setup/status');
  return data;
}


export async function bootstrapAdmin(
  email: string, password: string, fullName?: string,
): Promise<SetupStatus> {
  const { data } = await api.post<SetupStatus>('/api/v1/setup/bootstrap', {
    email, password, full_name: fullName,
  });
  return data;
}


export async function rotateEncryptionKey(): Promise<{ rotated: number; ok: boolean }> {
  const { data } = await api.post<{ rotated: number; ok: boolean }>(
    '/api/v1/settings/rotate-encryption-key',
  );
  return data;
}


// ─── Admin user management (self-hosted) ────────────────────────────────────

export interface ManagedUser {
  id: string;
  email: string;
  full_name: string | null;
  role: 'admin' | 'operator' | 'viewer';
  is_active: boolean;
}


export async function listUsers(): Promise<ManagedUser[]> {
  const { data } = await api.get<ManagedUser[]>('/api/v1/auth/users');
  return data;
}


export async function createUser(body: {
  email: string;
  password: string;
  full_name?: string;
  role: 'admin' | 'operator' | 'viewer';
}): Promise<ManagedUser> {
  const { data } = await api.post<ManagedUser>('/api/v1/auth/users', body);
  return data;
}


export async function updateUser(
  id: string,
  patch: { role?: string; is_active?: boolean; full_name?: string },
): Promise<ManagedUser> {
  const { data } = await api.patch<ManagedUser>(`/api/v1/auth/users/${id}`, patch);
  return data;
}


export async function deleteUser(id: string): Promise<void> {
  await api.delete(`/api/v1/auth/users/${id}`);
}


// ─── Migrations ─────────────────────────────────────────────────────────────

export interface MigrationSummary {
  id: string;
  name: string | null;
  source_schema: string | null;
  target_schema: string | null;
  status: string;
  rows_transferred: number;
  total_rows: number;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
}


export interface CheckpointSummary {
  table_name: string;
  rows_processed: number;
  total_rows: number;
  progress_percentage: number;
  status: string;
  last_rowid: string | null;
  error_message: string | null;
  updated_at: string | null;
}


export interface MigrationDetail extends MigrationSummary {
  source_url: string | null;
  target_url: string | null;
  tables: string[] | null;
  batch_size: number | null;
  create_tables: boolean;
  error_message: string | null;
  checkpoints: CheckpointSummary[];
}


export interface MigrationCreateBody {
  name: string;
  source_url: string;
  target_url: string;
  source_schema: string;
  target_schema: string;
  tables?: string[] | null;
  batch_size?: number;
  create_tables?: boolean;
}


export async function listMigrations(): Promise<MigrationSummary[]> {
  const { data } = await api.get<MigrationSummary[]>('/api/v1/migrations');
  return data;
}


export async function createMigration(
  body: MigrationCreateBody,
): Promise<MigrationSummary> {
  const { data } = await api.post<MigrationSummary>('/api/v1/migrations', body);
  return data;
}


export async function getMigration(id: string): Promise<MigrationDetail> {
  const { data } = await api.get<MigrationDetail>(`/api/v1/migrations/${id}`);
  return data;
}


export async function runMigration(id: string): Promise<MigrationSummary> {
  const { data } = await api.post<MigrationSummary>(`/api/v1/migrations/${id}/run`);
  return data;
}


export async function pollMigrationProgress(id: string): Promise<MigrationDetail> {
  const { data } = await api.get<MigrationDetail>(`/api/v1/migrations/${id}/progress`);
  return data;
}


export async function deleteMigration(id: string): Promise<void> {
  await api.delete(`/api/v1/migrations/${id}`);
}


export interface ConnectionTestResult {
  ok: boolean;
  dialect: 'oracle' | 'postgres' | null;
  message: string;
  schema: string | null;
  tables_found: number | null;
}


export async function testConnection(
  url: string,
  schema?: string,
): Promise<ConnectionTestResult> {
  const { data } = await api.post<ConnectionTestResult>(
    '/api/v1/migrations/test-connection',
    { url, schema: schema || undefined },
  );
  return data;
}


export interface MigrationPlan {
  tables_with_pk: string[];
  tables_skipped: string[];
  load_order: string[];
  create_table_ddl: string[];
  type_mappings: {
    table: string;
    column: string;
    source_type: string;
    pg_type: string;
  }[];
  deferred_constraints: string[];
}


export async function previewMigrationPlan(id: string): Promise<MigrationPlan> {
  const { data } = await api.post<MigrationPlan>(`/api/v1/migrations/${id}/plan`);
  return data;
}


// ─── Audit log ──────────────────────────────────────────────────────────────

export interface AuditEvent {
  id: string;
  user_email: string | null;
  action: string;
  resource_type: string | null;
  resource_id: string | null;
  details: Record<string, unknown> | null;
  ip: string | null;
  created_at: string;
}


export interface AuditPage {
  items: AuditEvent[];
  total: number;
  limit: number;
  offset: number;
}


export async function listAuditEvents(params: {
  action?: string;
  days?: number;
  limit?: number;
  offset?: number;
} = {}): Promise<AuditPage> {
  const { data } = await api.get<AuditPage>('/api/v1/audit', { params });
  return data;
}


export interface AuditVerifyResult {
  ok: boolean;
  checked: number;
  first_break: {
    id: string;
    action: string;
    created_at: string;
    expected: string;
    stored: string;
  } | null;
}


export async function verifyAuditChain(): Promise<AuditVerifyResult> {
  const { data } = await api.get<AuditVerifyResult>('/api/v1/audit/verify');
  return data;
}


// ─── SSO ────────────────────────────────────────────────────────────────────

export interface SsoPublicStatus {
  enabled: boolean;
  protocol: 'oidc' | 'saml' | null;
}


export interface SsoConfig {
  enabled: boolean;
  protocol: 'oidc' | 'saml' | string;
  default_role: 'admin' | 'operator' | 'viewer' | string;
  auto_provision: boolean;
  // OIDC
  issuer: string | null;
  client_id: string | null;
  client_secret_set: boolean;
  // SAML
  saml_entity_id: string | null;
  saml_sso_url: string | null;
  saml_x509_cert_set: boolean;
}


export async function getSsoPublicStatus(): Promise<SsoPublicStatus> {
  const { data } = await api.get<SsoPublicStatus>('/api/v1/auth/sso');
  return data;
}


export async function getSsoConfig(): Promise<SsoConfig> {
  const { data } = await api.get<SsoConfig>('/api/v1/auth/sso/config');
  return data;
}


export async function updateSsoConfig(patch: {
  enabled?: boolean;
  protocol?: 'oidc' | 'saml';
  default_role?: string;
  auto_provision?: boolean;
  issuer?: string;
  client_id?: string;
  client_secret?: string;
  saml_entity_id?: string;
  saml_sso_url?: string;
  saml_x509_cert?: string;
}): Promise<SsoConfig> {
  const { data } = await api.put<SsoConfig>('/api/v1/auth/sso/config', patch);
  return data;
}


export async function testSsoDiscovery(): Promise<{
  ok: boolean;
  authorization_endpoint: string;
  token_endpoint: string;
  userinfo_endpoint: string;
  issuer: string;
}> {
  const { data } = await api.post('/api/v1/auth/sso/test');
  return data;
}
