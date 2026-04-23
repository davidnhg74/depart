/**
 * White-label branding store.
 *
 * Mirrors the GET /api/v1/branding response shape. The API always
 * returns a fully-populated config — NULL columns on the server side
 * fall back to the bundled Hafen defaults, so the client never has to
 * juggle "have we loaded yet?" / "use default?" branching.
 *
 * The defaults below are duplicated from the server's
 * branding_service.DEFAULT_* constants so the UI can render before the
 * /branding fetch completes (first paint shows the Hafen brand; if a
 * white-label config is in place, the second paint swaps it in).
 *
 * The store also stamps the primary color onto the document root as a
 * CSS custom property `--brand-primary` so plain Tailwind classes can
 * pick it up via `[color:var(--brand-primary)]` etc.
 */
import { create } from 'zustand';

export interface Branding {
  company_name: string;
  product_name: string;
  logo_url: string | null;
  primary_color: string;
  support_email: string;
  white_label_enabled: boolean;
}

const DEFAULT_BRANDING: Branding = {
  company_name: 'Hafen',
  product_name: 'Hafen',
  logo_url: null,
  primary_color: '#7C3AED',
  support_email: 'support@hafen.ai',
  white_label_enabled: false,
};

interface BrandingStore {
  branding: Branding;
  loaded: boolean;
  setBranding: (b: Branding) => void;
}

export const useBrandingStore = create<BrandingStore>((set) => ({
  branding: DEFAULT_BRANDING,
  loaded: false,
  setBranding: (b) => {
    set({ branding: b, loaded: true });
    // Make the primary color available to CSS as a custom property so
    // a chunk of the UI can theme without each component re-reading
    // the store. Guarded for SSR — `document` doesn't exist there.
    if (typeof document !== 'undefined') {
      document.documentElement.style.setProperty(
        '--brand-primary',
        b.primary_color,
      );
    }
  },
}));
