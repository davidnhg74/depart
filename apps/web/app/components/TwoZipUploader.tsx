/**
 * Side-by-side dual zip upload for the analyzer + runbook flows.
 *
 * Source-zip is optional — the runbook generator can build a deterministic
 * runbook from schema alone. The component handles its own validation
 * (zip-only, size cap) and reports the chosen Files up to the parent.
 */
'use client';

import React, { useRef, useState } from 'react';

const MAX_BYTES = 100 * 1024 * 1024;

interface Props {
  schema: File | null;
  source: File | null;
  onSchema: (f: File | null) => void;
  onSource: (f: File | null) => void;
  schemaRequired?: boolean;
  sourceRequired?: boolean;
  disabled?: boolean;
}

export default function TwoZipUploader({
  schema, source, onSchema, onSource,
  schemaRequired = true, sourceRequired = false,
  disabled = false,
}: Props) {
  return (
    <div className="grid md:grid-cols-2 gap-4">
      <ZipPicker
        label="Oracle DDL (.zip)"
        hint="Required: SQL files defining your schema (CREATE TABLE/PACKAGE/...)"
        file={schema}
        onPick={onSchema}
        required={schemaRequired}
        disabled={disabled}
        testId="schema-picker"
      />
      <ZipPicker
        label="Application Source (.zip)"
        hint="Optional: .java / .py / .cs / .xml — used for app-impact analysis"
        file={source}
        onPick={onSource}
        required={sourceRequired}
        disabled={disabled}
        testId="source-picker"
      />
    </div>
  );
}

function ZipPicker({
  label, hint, file, onPick, required, disabled, testId,
}: {
  label: string;
  hint: string;
  file: File | null;
  onPick: (f: File | null) => void;
  required: boolean;
  disabled: boolean;
  testId: string;
}) {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [error, setError] = useState<string>('');
  const [isDragging, setIsDragging] = useState(false);

  function handle(file: File | null) {
    setError('');
    if (file == null) {
      onPick(null);
      return;
    }
    if (!file.name.toLowerCase().endsWith('.zip')) {
      setError('File must be a .zip');
      return;
    }
    if (file.size > MAX_BYTES) {
      setError(`File exceeds ${MAX_BYTES / (1024 * 1024)} MB cap`);
      return;
    }
    onPick(file);
  }

  return (
    <div data-testid={testId} className="space-y-2">
      <div className="flex items-baseline justify-between">
        <label className="text-sm font-semibold text-gray-800">
          {label} {required && <span className="text-red-600">*</span>}
        </label>
        {file && (
          <button
            type="button"
            onClick={() => { handle(null); if (inputRef.current) inputRef.current.value = ''; }}
            className="text-xs text-gray-500 hover:text-gray-800"
          >
            Clear
          </button>
        )}
      </div>
      <div
        onDragOver={(e) => { e.preventDefault(); if (!disabled) setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setIsDragging(false);
          if (disabled) return;
          if (e.dataTransfer.files.length > 0) handle(e.dataTransfer.files[0]);
        }}
        onClick={() => !disabled && inputRef.current?.click()}
        className={`cursor-pointer border-2 border-dashed rounded-lg p-4 text-center transition
          ${isDragging ? 'border-purple-500 bg-purple-50' : 'border-gray-300 hover:border-purple-400'}
          ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
      >
        <input
          ref={inputRef}
          type="file"
          // No `accept` — JS validation in handle() is the source of truth
          // (drag-and-drop bypasses accept anyway, and the attribute is only
          // a UI hint not a security control). Keeps rejection paths testable.
          className="hidden"
          disabled={disabled}
          onChange={(e) => handle(e.target.files?.[0] ?? null)}
          data-testid={`${testId}-input`}
        />
        {file ? (
          <div className="text-sm text-gray-900">
            <div className="font-mono truncate">{file.name}</div>
            <div className="text-xs text-gray-500 mt-1">
              {(file.size / 1024).toFixed(1)} KB
            </div>
          </div>
        ) : (
          <div className="text-sm text-gray-700">
            <div>Drop a .zip here, or click to browse</div>
            <div className="text-xs text-gray-500 mt-1">{hint}</div>
          </div>
        )}
      </div>
      {error && (
        <p data-testid={`${testId}-error`} className="text-sm text-red-700">
          {error}
        </p>
      )}
    </div>
  );
}
