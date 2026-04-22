/**Auth store using Zustand - manages user state and authentication*/
import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export interface User {
  id: string;
  email: string;
  full_name: string;
  plan: 'trial' | 'starter' | 'professional' | 'enterprise';
  email_verified: boolean;
  created_at: string;
}

interface AuthStore {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;

  // Actions
  setUser: (user: User | null) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  logout: () => void;
  refreshUser: (user: User | null) => void;
  clearError: () => void;
}

export const useAuthStore = create<AuthStore>()(
  persist(
    (set) => ({
      user: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,

      setUser: (user) =>
        set({
          user,
          isAuthenticated: !!user,
          error: null,
        }),

      setLoading: (isLoading) => set({ isLoading }),

      setError: (error) => set({ error }),

      logout: () =>
        set({
          user: null,
          isAuthenticated: false,
          error: null,
        }),

      refreshUser: (user) =>
        set({
          user,
          isAuthenticated: !!user,
        }),

      clearError: () => set({ error: null }),
    }),
    {
      name: 'auth-store', // LocalStorage key
      partialize: (state) => ({ user: state.user }),
    }
  )
);
