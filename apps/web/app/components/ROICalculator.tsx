'use client';

import { useState } from 'react';

/*
 * Oracle support pricing assumptions (publicly documented):
 *
 * Oracle EE processor license  ~$47,500 / CPU core  (Oracle List Price)
 * Oracle SE2 socket license     ~$17,500 / socket    (max 2 sockets)
 * Oracle Premier Support          22% of license/yr   (standard rate)
 * Oracle options (RAC, Part.)   adds ~50% to EE base
 *
 * We model annual support cost = CPUs × per-CPU-license × 22%.
 * This is conservative — many shops compound at 8%/yr and carry
 * options that double the effective rate.
 */

const EDITIONS = [
  {
    id: 'ee',
    label: 'Enterprise Edition',
    licensePerCPU: 47500,
    note: 'Per-core processor licensing',
  },
  {
    id: 'se2',
    label: 'Standard Edition 2',
    licensePerCPU: 17500,
    note: 'Per socket (max 2 sockets)',
  },
] as const;

type EditionId = (typeof EDITIONS)[number]['id'];

function fmt(n: number) {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${Math.round(n / 1_000)}k`;
  return `$${Math.round(n)}`;
}

export default function ROICalculator() {
  const [cpus, setCpus] = useState(32);
  const [edition, setEdition] = useState<EditionId>('ee');
  const [includeOptions, setIncludeOptions] = useState(false);
  const [years, setYears] = useState(5);

  const ed = EDITIONS.find((e) => e.id === edition)!;
  const licenseBase = cpus * ed.licensePerCPU;
  const licenseTotal = licenseBase * (includeOptions ? 1.5 : 1);
  const annualSupport = licenseTotal * 0.22;
  const totalOracle = annualSupport * years;

  // Hafen migration: one-time Pro license. For >64 CPUs → Enterprise tier.
  const hafenCost = cpus > 64 ? 75_000 : 50_000;
  // PostgreSQL managed infra: ~$3k/yr (Neon/RDS equivalent, conservative)
  const pgAnnual = 3_000;
  const totalPg = hafenCost + pgAnnual * years;
  const savings = totalOracle - totalPg;
  const paybackMonths = savings > 0 ? Math.ceil((hafenCost / (annualSupport - pgAnnual)) * 12) : null;

  return (
    <section className="bg-gradient-to-br from-purple-50 to-blue-50 py-20">
      <div className="container mx-auto max-w-5xl px-4">
        <div className="text-center">
          <h2 className="text-3xl font-bold text-gray-900 md:text-4xl">
            How much is Oracle costing you?
          </h2>
          <p className="mt-3 text-gray-600">
            Adjust the inputs below — we use Oracle&apos;s published list prices and the
            standard 22% annual support rate.
          </p>
        </div>

        <div className="mt-10 grid grid-cols-1 gap-8 md:grid-cols-2">
          {/* ── Controls ── */}
          <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
            <h3 className="mb-5 font-semibold text-gray-900">Your Oracle environment</h3>

            {/* Edition toggle */}
            <div className="mb-6">
              <label className="mb-2 block text-sm font-medium text-gray-700">
                Oracle edition
              </label>
              <div className="flex gap-2">
                {EDITIONS.map((e) => (
                  <button
                    key={e.id}
                    onClick={() => setEdition(e.id)}
                    className={`flex-1 rounded-lg border px-3 py-2 text-sm font-medium transition ${
                      edition === e.id
                        ? 'border-purple-600 bg-purple-600 text-white'
                        : 'border-gray-200 bg-white text-gray-700 hover:border-purple-300'
                    }`}
                  >
                    {e.label}
                    <span className="block text-xs font-normal opacity-70">{e.note}</span>
                  </button>
                ))}
              </div>
            </div>

            {/* CPU count */}
            <div className="mb-6">
              <div className="mb-2 flex justify-between">
                <label className="text-sm font-medium text-gray-700">
                  {edition === 'ee' ? 'CPU cores' : 'CPU sockets'}
                </label>
                <span className="text-sm font-semibold text-purple-700">{cpus}</span>
              </div>
              <input
                type="range"
                min={edition === 'se2' ? 1 : 1}
                max={edition === 'se2' ? 2 : 256}
                value={cpus}
                onChange={(e) => setCpus(Number(e.target.value))}
                className="w-full accent-purple-600"
              />
              <div className="mt-1 flex justify-between text-xs text-gray-400">
                <span>1</span>
                <span>{edition === 'se2' ? '2 (max)' : '256'}</span>
              </div>
            </div>

            {/* Include options */}
            {edition === 'ee' && (
              <div className="mb-6">
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={includeOptions}
                    onChange={(e) => setIncludeOptions(e.target.checked)}
                    className="h-4 w-4 accent-purple-600"
                  />
                  <span className="text-sm text-gray-700">
                    Include common options
                    <span className="ml-1 text-xs text-gray-500">(RAC + Partitioning +50%)</span>
                  </span>
                </label>
              </div>
            )}

            {/* Projection period */}
            <div>
              <div className="mb-2 flex justify-between">
                <label className="text-sm font-medium text-gray-700">Projection</label>
                <span className="text-sm font-semibold text-purple-700">{years} years</span>
              </div>
              <div className="flex gap-2">
                {[3, 5, 7].map((y) => (
                  <button
                    key={y}
                    onClick={() => setYears(y)}
                    className={`flex-1 rounded-lg border py-1.5 text-sm font-medium transition ${
                      years === y
                        ? 'border-purple-600 bg-purple-600 text-white'
                        : 'border-gray-200 bg-white text-gray-700 hover:border-purple-300'
                    }`}
                  >
                    {y}yr
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* ── Results ── */}
          <div className="rounded-2xl border border-purple-200 bg-purple-900 p-6 text-white shadow-sm">
            <h3 className="mb-5 font-semibold text-purple-200">
              {years}-year cost comparison
            </h3>

            {/* Oracle stack */}
            <div className="space-y-3 border-b border-purple-700 pb-5">
              <div className="flex items-baseline justify-between">
                <span className="text-sm text-purple-300">Oracle license basis</span>
                <span className="font-mono font-semibold">{fmt(licenseTotal)}</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-sm text-purple-300">Annual support (22%)</span>
                <span className="font-mono font-semibold text-red-300">{fmt(annualSupport)}/yr</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-sm font-medium text-purple-200">{years}-yr Oracle spend</span>
                <span className="font-mono text-xl font-bold text-red-300">{fmt(totalOracle)}</span>
              </div>
            </div>

            {/* Hafen + PG stack */}
            <div className="mt-5 space-y-3 border-b border-purple-700 pb-5">
              <div className="flex items-baseline justify-between">
                <span className="text-sm text-purple-300">Hafen migration (one-time)</span>
                <span className="font-mono font-semibold">{fmt(hafenCost)}</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-sm text-purple-300">PostgreSQL infra (~est.)</span>
                <span className="font-mono font-semibold">{fmt(pgAnnual)}/yr</span>
              </div>
              <div className="flex items-baseline justify-between">
                <span className="text-sm font-medium text-purple-200">{years}-yr after migration</span>
                <span className="font-mono text-xl font-bold text-green-300">{fmt(totalPg)}</span>
              </div>
            </div>

            {/* Savings */}
            <div className="mt-5">
              {savings > 0 ? (
                <>
                  <div className="flex items-baseline justify-between">
                    <span className="text-lg font-bold text-white">{years}-yr net savings</span>
                    <span className="font-mono text-2xl font-black text-green-300">
                      {fmt(savings)}
                    </span>
                  </div>
                  {paybackMonths !== null && paybackMonths > 0 && (
                    <p className="mt-2 text-xs text-purple-300">
                      Hafen license pays back in ~{paybackMonths} month{paybackMonths !== 1 ? 's' : ''} of avoided Oracle support.
                    </p>
                  )}
                </>
              ) : (
                <p className="text-sm text-purple-300">
                  Increase CPU count or extend the projection to see savings.
                </p>
              )}
            </div>

            <p className="mt-6 text-xs text-purple-400 leading-relaxed">
              Based on Oracle published list prices. Actual costs vary by negotiated discounts,
              ULA terms, and support tier. PostgreSQL estimate uses managed cloud pricing
              (Neon/RDS). Hafen self-hosted pricing at{' '}
              <a href="/pricing" className="underline opacity-80 hover:opacity-100">
                hafen.ai/pricing
              </a>
              .
            </p>
          </div>
        </div>
      </div>
    </section>
  );
}
