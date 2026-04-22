'use client';

import Editor, { DiffEditor } from '@monaco-editor/react';
import { useState } from 'react';

interface DiffViewerProps {
  originalCode: string;
  convertedCode: string;
  language?: 'sql' | 'plpgsql' | 'oracle';
  title?: string;
  readOnly?: boolean;
  onConverted?: () => void;
}

export default function DiffViewer({
  originalCode,
  convertedCode,
  language = 'sql',
  title = 'Oracle → PostgreSQL Conversion',
  readOnly = true,
  onConverted,
}: DiffViewerProps) {
  const [activeTab, setActiveTab] = useState<'side-by-side' | 'original' | 'converted'>('side-by-side');

  const editorOptions = {
    readOnly,
    minimap: { enabled: false },
    fontSize: 13,
    fontFamily: 'Fira Code, Menlo, monospace',
    lineNumbers: 'on' as const,
    wordWrap: 'on' as const,
    scrollBeyondLastLine: false,
  };

  return (
    <div className="w-full bg-white rounded-lg shadow-lg p-6">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-2xl font-bold text-gray-900 mb-2">{title}</h2>
        <p className="text-sm text-gray-600">
          Left: Oracle PL/SQL | Right: PostgreSQL PL/pgSQL
        </p>
      </div>

      {/* View Toggle */}
      <div className="flex gap-2 mb-4 border-b border-gray-200">
        <button
          onClick={() => setActiveTab('side-by-side')}
          className={`px-4 py-2 font-medium text-sm transition ${
            activeTab === 'side-by-side'
              ? 'border-b-2 border-purple-600 text-purple-600'
              : 'text-gray-600 hover:text-gray-900'
          }`}
        >
          Side-by-Side
        </button>
        <button
          onClick={() => setActiveTab('original')}
          className={`px-4 py-2 font-medium text-sm transition ${
            activeTab === 'original'
              ? 'border-b-2 border-blue-600 text-blue-600'
              : 'text-gray-600 hover:text-gray-900'
          }`}
        >
          Oracle (Original)
        </button>
        <button
          onClick={() => setActiveTab('converted')}
          className={`px-4 py-2 font-medium text-sm transition ${
            activeTab === 'converted'
              ? 'border-b-2 border-green-600 text-green-600'
              : 'text-gray-600 hover:text-gray-900'
          }`}
        >
          PostgreSQL (Converted)
        </button>
      </div>

      {/* Side-by-Side View with Diff Highlighting */}
      {activeTab === 'side-by-side' && (
        <div className="border border-gray-200 rounded-lg overflow-hidden h-96">
          <DiffEditor
            height="100%"
            original={originalCode}
            modified={convertedCode}
            language="sql"
            theme="light"
            options={{
              ...editorOptions,
              renderSideBySide: true,
              readOnly: true,
              scrollBeyondLastLine: false,
            }}
          />
        </div>
      )}

      {/* Original Only */}
      {activeTab === 'original' && (
        <div className="border border-gray-200 rounded-lg overflow-hidden h-96">
          <div className="bg-blue-50 px-3 py-2 border-b border-gray-200">
            <h3 className="font-semibold text-blue-900">Oracle PL/SQL</h3>
          </div>
          <Editor
            height="100%"
            defaultLanguage="sql"
            value={originalCode}
            options={editorOptions}
            theme="light"
          />
        </div>
      )}

      {/* Converted Only */}
      {activeTab === 'converted' && (
        <div className="border border-gray-200 rounded-lg overflow-hidden h-96">
          <div className="bg-green-50 px-3 py-2 border-b border-gray-200">
            <h3 className="font-semibold text-green-900">PostgreSQL PL/pgSQL</h3>
          </div>
          <Editor
            height="100%"
            defaultLanguage="plpgsql"
            value={convertedCode}
            options={editorOptions}
            theme="light"
          />
        </div>
      )}

      {/* Analysis */}
      <ConversionAnalysis original={originalCode} converted={convertedCode} />
    </div>
  );
}

function ConversionAnalysis({ original, converted }: { original: string; converted: string }) {
  const calculateStats = () => {
    const originalLines = original.split('\n').length;
    const convertedLines = converted.split('\n').length;
    const lineDiff = convertedLines - originalLines;

    const originalChars = original.length;
    const convertedChars = converted.length;
    const charDiff = convertedChars - originalChars;

    return {
      originalLines,
      convertedLines,
      lineDiff,
      originalChars,
      convertedChars,
      charDiff,
    };
  };

  const stats = calculateStats();

  return (
    <div className="mt-6 grid grid-cols-2 gap-4">
      <div className="bg-gray-50 p-4 rounded-lg">
        <h4 className="font-semibold text-gray-900 mb-3">Code Statistics</h4>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-600">Original Lines:</span>
            <span className="font-medium">{stats.originalLines}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Converted Lines:</span>
            <span className="font-medium">{stats.convertedLines}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Line Change:</span>
            <span className={`font-medium ${stats.lineDiff > 0 ? 'text-orange-600' : 'text-green-600'}`}>
              {stats.lineDiff > 0 ? '+' : ''}{stats.lineDiff}
            </span>
          </div>
        </div>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg">
        <h4 className="font-semibold text-gray-900 mb-3">Character Count</h4>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-600">Original:</span>
            <span className="font-medium">{stats.originalChars.toLocaleString()} chars</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Converted:</span>
            <span className="font-medium">{stats.convertedChars.toLocaleString()} chars</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600">Change:</span>
            <span className={`font-medium ${stats.charDiff > 0 ? 'text-orange-600' : 'text-green-600'}`}>
              {stats.charDiff > 0 ? '+' : ''}{stats.charDiff}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}
