/**
 * App-Impact analyzer page.
 *
 * Customer uploads (1) Oracle DDL zip and (2) application source zip,
 * we POST /api/v3/analyze/app-impact, render the structured findings
 * grouped by file. Same upload bundle drives the runbook PDF download —
 * one button at the top of the results view fires
 * /api/v3/projects/runbook and saves the file.
 */
'use client';

import React, { useState } from 'react';
import axios from 'axios';

import AuthGuard from '@/app/components/AuthGuard';
import FindingsList, { AppImpactResponse } from '@/app/components/FindingsList';
import TwoZipUploader from '@/app/components/TwoZipUploader';
import { apiBaseUrl } from '@/app/lib/api';

function PageContent() {
  const [schemaZip, setSchemaZip] = useState<File | null>(null);
  const [sourceZip, setSourceZip] = useState<File | null>(null);
  const [explain, setExplain] = useState(false);
  const [languages, setLanguages] = useState('');
  const [report, setReport] = useState<AppImpactResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const [projectName, setProjectName] = useState('');
  const [customer, setCustomer] = useState('');
  const [downloadingPdf, setDownloadingPdf] = useState(false);

  async function runAnalysis() {
    setError('');
    if (!schemaZip || !sourceZip) {
      setError('Both schema and source zips are required.');
      return;
    }
    setLoading(true);
    setReport(null);
    try {
      const fd = new FormData();
      fd.append('schema_zip', schemaZip);
      fd.append('source_zip', sourceZip);
      fd.append('explain', String(explain));
      if (languages.trim()) fd.append('languages', languages.trim());
      const resp = await axios.post<AppImpactResponse>(
        `${apiBaseUrl()}/api/v3/analyze/app-impact`, fd
      );
      setReport(resp.data);
    } catch (err) {
      setError(messageFromError(err));
    } finally {
      setLoading(false);
    }
  }

  async function downloadRunbook() {
    setError('');
    if (!schemaZip) {
      setError('Schema zip is required to generate a runbook.');
      return;
    }
    if (!projectName.trim() || !customer.trim()) {
      setError('Project name and customer name are required for the runbook.');
      return;
    }
    setDownloadingPdf(true);
    try {
      const fd = new FormData();
      fd.append('schema_zip', schemaZip);
      if (sourceZip) fd.append('source_zip', sourceZip);
      fd.append('project_name', projectName);
      fd.append('customer', customer);
      fd.append('explain', String(explain));
      fd.append('format', 'pdf');
      const resp = await axios.post(
        `${apiBaseUrl()}/api/v3/projects/runbook`, fd, { responseType: 'blob' }
      );
      saveBlob(resp.data, `runbook-${customer.replace(/\s+/g, '_')}.pdf`);
    } catch (err) {
      setError(messageFromError(err));
    } finally {
      setDownloadingPdf(false);
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50">
      <header className="bg-gradient-to-r from-purple-600 to-blue-600 text-white py-8">
        <div className="container mx-auto px-4">
          <h1 className="text-3xl font-bold">Application Impact Analyzer</h1>
          <p className="mt-2 text-purple-100">
            Find every Oracle-specific call site in your application code that will
            need to change post-migration. Generate a customer-deliverable runbook
            from the same uploads.
          </p>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8 space-y-6">
        <section className="bg-white border border-gray-200 rounded-lg p-6 space-y-4">
          <h2 className="text-lg font-semibold text-gray-900">1. Upload</h2>
          <TwoZipUploader
            schema={schemaZip} source={sourceZip}
            onSchema={setSchemaZip} onSource={setSourceZip}
            schemaRequired sourceRequired
            disabled={loading || downloadingPdf}
          />

          <div className="grid md:grid-cols-2 gap-4">
            <label className="text-sm">
              <span className="block font-semibold text-gray-800 mb-1">
                Languages (optional)
              </span>
              <input
                type="text"
                value={languages}
                onChange={(e) => setLanguages(e.target.value)}
                placeholder="java,python,csharp,mybatis"
                disabled={loading || downloadingPdf}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-purple-500"
                data-testid="languages-input"
              />
            </label>
            <label className="text-sm flex items-end gap-2">
              <input
                type="checkbox"
                checked={explain}
                onChange={(e) => setExplain(e.target.checked)}
                disabled={loading || downloadingPdf}
                className="h-4 w-4 text-purple-600 border-gray-300 rounded"
                data-testid="explain-toggle"
              />
              <span className="text-sm text-gray-800">
                Add AI-written explanation, before/after code, and caveats per
                finding (uses your Anthropic credits)
              </span>
            </label>
          </div>

          <div className="pt-2">
            <button
              onClick={runAnalysis}
              disabled={loading || downloadingPdf || !schemaZip || !sourceZip}
              data-testid="run-analysis"
              className="px-4 py-2 bg-purple-600 text-white rounded-md hover:bg-purple-700 disabled:bg-gray-400 font-medium"
            >
              {loading ? 'Analyzing…' : 'Run Impact Analysis'}
            </button>
          </div>
        </section>

        <section className="bg-white border border-gray-200 rounded-lg p-6 space-y-4">
          <h2 className="text-lg font-semibold text-gray-900">2. Generate Runbook PDF (optional)</h2>
          <p className="text-sm text-gray-600">
            Same uploads as above, plus project metadata. Returns a customer-deliverable PDF.
          </p>
          <div className="grid md:grid-cols-2 gap-4">
            <label className="text-sm">
              <span className="block font-semibold text-gray-800 mb-1">Project name</span>
              <input
                type="text"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                placeholder="ACME OLTP Migration"
                disabled={downloadingPdf}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-purple-500"
                data-testid="project-name-input"
              />
            </label>
            <label className="text-sm">
              <span className="block font-semibold text-gray-800 mb-1">Customer</span>
              <input
                type="text"
                value={customer}
                onChange={(e) => setCustomer(e.target.value)}
                placeholder="ACME Corp"
                disabled={downloadingPdf}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-purple-500"
                data-testid="customer-input"
              />
            </label>
          </div>
          <div>
            <button
              onClick={downloadRunbook}
              disabled={downloadingPdf || !schemaZip || !projectName.trim() || !customer.trim()}
              data-testid="download-runbook"
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 font-medium"
            >
              {downloadingPdf ? 'Generating…' : 'Download Runbook PDF'}
            </button>
          </div>
        </section>

        {error && (
          <div data-testid="error" className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-800">
            {error}
          </div>
        )}

        {report && (
          <section className="space-y-4">
            <h2 className="text-lg font-semibold text-gray-900">Findings</h2>
            <FindingsList report={report} />
          </section>
        )}
      </main>
    </div>
  );
}

function messageFromError(err: unknown): string {
  if (axios.isAxiosError(err)) {
    return (err.response?.data?.detail as string) || err.message || 'Request failed.';
  }
  return 'Unexpected error. Check the browser console.';
}

function saveBlob(blob: Blob, filename: string): void {
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);
}

export default function AppImpactPage() {
  return (
    <AuthGuard>
      <PageContent />
    </AuthGuard>
  );
}
