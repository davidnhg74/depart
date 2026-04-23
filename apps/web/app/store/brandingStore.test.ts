/**
 * Tests for the branding store.
 *
 * The store ships with Hafen defaults and gets hydrated by
 * BrandingInitializer at layout-mount time. We assert on the default
 * shape and on the side-effect of stamping --brand-primary onto the
 * document root, since that's the contract the rest of the UI relies
 * on for theming.
 */
import { describe, it, expect, beforeEach } from 'vitest';

import { useBrandingStore } from './brandingStore';


describe('brandingStore', () => {
  beforeEach(() => {
    // Reset to defaults between tests — Zustand stores are
    // process-global, so otherwise a setBranding in one test bleeds
    // into the next.
    useBrandingStore.setState({
      branding: {
        company_name: 'Hafen',
        product_name: 'Hafen',
        logo_url: null,
        primary_color: '#7C3AED',
        support_email: 'support@hafen.ai',
        white_label_enabled: false,
      },
      loaded: false,
    });
    document.documentElement.style.removeProperty('--brand-primary');
  });

  it('seeds with the Hafen defaults so first paint is correct', () => {
    const { branding, loaded } = useBrandingStore.getState();
    expect(branding.product_name).toBe('Hafen');
    expect(branding.primary_color).toBe('#7C3AED');
    expect(branding.support_email).toBe('support@hafen.ai');
    expect(loaded).toBe(false);
  });

  it('setBranding hydrates and flips loaded=true', () => {
    useBrandingStore.getState().setBranding({
      company_name: 'Acme Corp',
      product_name: 'Acme Migrator',
      logo_url: 'https://acme.example.com/logo.png',
      primary_color: '#FF8800',
      support_email: 'help@acme.example.com',
      white_label_enabled: true,
    });
    const { branding, loaded } = useBrandingStore.getState();
    expect(branding.product_name).toBe('Acme Migrator');
    expect(branding.logo_url).toBe('https://acme.example.com/logo.png');
    expect(loaded).toBe(true);
  });

  it('stamps the primary color onto the document root', () => {
    // The CSS custom property `--brand-primary` is what other
    // components opt into via `style={{ color: var(--brand-primary) }}`.
    useBrandingStore.getState().setBranding({
      company_name: 'X',
      product_name: 'X',
      logo_url: null,
      primary_color: '#123456',
      support_email: 'x@x.com',
      white_label_enabled: true,
    });
    expect(
      document.documentElement.style.getPropertyValue('--brand-primary'),
    ).toBe('#123456');
  });
});
