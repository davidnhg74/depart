'use client';

import { useState } from 'react';
import axios from 'axios';
import DiffViewer from '../components/DiffViewer';
import SemanticIssuesPanel from '../components/SemanticIssuesPanel';
import Editor from '@monaco-editor/react';

type ConstructType = 'PROCEDURE' | 'FUNCTION' | 'TABLE' | 'VIEW' | 'SEQUENCE' | 'INDEX';

export default function ConvertPage() {
  const [inputCode, setInputCode] = useState('');
  const [constructType, setConstructType] = useState<ConstructType>('PROCEDURE');
  const [outputCode, setOutputCode] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [warnings, setWarnings] = useState<string[]>([]);
  const [errors, setErrors] = useState<string[]>([]);
  const [method, setMethod] = useState('');
  const [success, setSuccess] = useState(false);

  const handleConvert = async () => {
    if (!inputCode.trim()) {
      setError('Please enter code to convert');
      return;
    }

    setIsLoading(true);
    setError('');
    setWarnings([]);
    setErrors([]);

    try {
      const endpoint =
        constructType === 'PROCEDURE' || constructType === 'FUNCTION'
          ? '/api/v2/convert/plsql'
          : '/api/v2/convert/schema';

      const response = await axios.post(
        `${process.env.NEXT_PUBLIC_API_URL}${endpoint}`,
        {
          code: inputCode,
          construct_type: constructType,
        }
      );

      setOutputCode(response.data.converted);
      setSuccess(response.data.success);
      setMethod(response.data.method);
      setWarnings(response.data.warnings || []);
      setErrors(response.data.errors || []);
    } catch (err) {
      const errorMsg = axios.isAxiosError(err)
        ? err.response?.data?.detail || 'Conversion failed'
        : 'An error occurred';
      setError(errorMsg);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCopyToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(outputCode);
      alert('Converted code copied to clipboard!');
    } catch (err) {
      alert('Failed to copy code');
    }
  };

  const handleDownload = () => {
    const element = document.createElement('a');
    element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(outputCode));
    element.setAttribute('download', `converted_${constructType.toLowerCase()}.sql`);
    element.style.display = 'none';
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-50 to-blue-50">
      {/* Header */}
      <header className="bg-gradient-to-r from-purple-600 to-blue-600 text-white py-8">
        <div className="container mx-auto px-4">
          <h1 className="text-4xl font-bold mb-2">PL/SQL Converter</h1>
          <p className="text-purple-100">
            Transform Oracle PL/SQL to PostgreSQL PL/pgSQL with confidence
          </p>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
          {/* Input Panel */}
          <div className="lg:col-span-1 space-y-4">
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-lg font-bold text-gray-900 mb-4">Conversion Settings</h2>

              {/* Construct Type Selection */}
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Construct Type
                </label>
                <select
                  value={constructType}
                  onChange={(e) => setConstructType(e.target.value as ConstructType)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                  disabled={isLoading || success}
                >
                  <option value="PROCEDURE">Procedure</option>
                  <option value="FUNCTION">Function</option>
                  <option value="TABLE">Table</option>
                  <option value="VIEW">View</option>
                  <option value="SEQUENCE">Sequence</option>
                  <option value="INDEX">Index</option>
                </select>
              </div>

              {/* Buttons */}
              <div className="space-y-2">
                <button
                  onClick={handleConvert}
                  disabled={isLoading}
                  className="w-full bg-purple-600 text-white px-4 py-2 rounded-lg hover:bg-purple-700 disabled:bg-gray-400 transition font-medium"
                >
                  {isLoading ? 'Converting...' : 'Convert'}
                </button>

                {success && (
                  <>
                    <button
                      onClick={handleCopyToClipboard}
                      className="w-full bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition font-medium"
                    >
                      Copy to Clipboard
                    </button>
                    <button
                      onClick={handleDownload}
                      className="w-full bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition font-medium"
                    >
                      Download
                    </button>
                  </>
                )}
              </div>

              {/* Status */}
              {success && (
                <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-lg">
                  <p className="text-sm text-green-800 font-medium">✓ Conversion Successful</p>
                  <p className="text-xs text-green-700 mt-1">Method: {method}</p>
                </div>
              )}

              {error && (
                <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                  <p className="text-sm text-red-800 font-medium">✗ Error</p>
                  <p className="text-xs text-red-700 mt-1">{error}</p>
                </div>
              )}

              {/* Warnings */}
              {warnings.length > 0 && (
                <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                  <p className="text-sm text-yellow-800 font-medium">⚠ Warnings ({warnings.length})</p>
                  <ul className="text-xs text-yellow-700 mt-2 space-y-1">
                    {warnings.slice(0, 5).map((w, i) => (
                      <li key={i}>• {w}</li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Errors */}
              {errors.length > 0 && (
                <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                  <p className="text-sm text-red-800 font-medium">✗ Issues ({errors.length})</p>
                  <ul className="text-xs text-red-700 mt-2 space-y-1">
                    {errors.slice(0, 5).map((e, i) => (
                      <li key={i}>• {e}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </div>

          {/* Code Editor Panel */}
          <div className="lg:col-span-3 bg-white rounded-lg shadow overflow-hidden">
            {!success ? (
              <div className="space-y-4 p-6">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Oracle PL/SQL Code
                  </label>
                  <Editor
                    height="400px"
                    defaultLanguage="sql"
                    value={inputCode}
                    onChange={(value) => setInputCode(value || '')}
                    options={{
                      minimap: { enabled: false },
                      fontSize: 13,
                      fontFamily: 'Fira Code, Menlo, monospace',
                      wordWrap: 'on',
                    }}
                    theme="light"
                  />
                </div>

                {/* Template Examples */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Quick Templates
                  </label>
                  <div className="flex gap-2 flex-wrap">
                    {TEMPLATES[constructType]?.map((template) => (
                      <button
                        key={template.name}
                        onClick={() => setInputCode(template.code)}
                        className="text-xs px-3 py-1 bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition"
                      >
                        {template.name}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="space-y-8">
                <DiffViewer
                  originalCode={inputCode}
                  convertedCode={outputCode}
                  language="sql"
                  title="Conversion Result"
                />
                <SemanticIssuesPanel
                  oracleDdl={inputCode}
                  pgDdl={outputCode}
                  autoAnalyze={success && constructType === 'TABLE'}
                />
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

const TEMPLATES: Record<ConstructType, Array<{ name: string; code: string }>> = {
  PROCEDURE: [
    {
      name: 'Simple Proc',
      code: `CREATE OR REPLACE PROCEDURE greet(p_name VARCHAR2) AS
BEGIN
  DBMS_OUTPUT.PUT_LINE('Hello ' || p_name);
END greet;`,
    },
    {
      name: 'Proc with INSERT',
      code: `CREATE OR REPLACE PROCEDURE insert_emp(p_name VARCHAR2, p_salary NUMBER) AS
BEGIN
  INSERT INTO employees (first_name, salary) VALUES (p_name, p_salary);
  COMMIT;
END insert_emp;`,
    },
  ],
  FUNCTION: [
    {
      name: 'Simple Func',
      code: `CREATE OR REPLACE FUNCTION double_it(p_val NUMBER) RETURN NUMBER AS
BEGIN
  RETURN p_val * 2;
END double_it;`,
    },
    {
      name: 'Func with Query',
      code: `CREATE OR REPLACE FUNCTION get_salary(p_emp_id NUMBER) RETURN NUMBER AS
  v_salary employees.salary%TYPE;
BEGIN
  SELECT salary INTO v_salary FROM employees WHERE employee_id = p_emp_id;
  RETURN NVL(v_salary, 0);
END get_salary;`,
    },
  ],
  TABLE: [
    {
      name: 'Simple Table',
      code: `CREATE TABLE employees (
  employee_id NUMBER(6) PRIMARY KEY,
  first_name VARCHAR2(50) NOT NULL,
  salary NUMBER(10,2)
);`,
    },
    {
      name: 'Table with FK',
      code: `CREATE TABLE employees (
  employee_id NUMBER(6) PRIMARY KEY,
  first_name VARCHAR2(50),
  department_id NUMBER(4),
  CONSTRAINT fk_dept FOREIGN KEY (department_id) REFERENCES departments(department_id)
);`,
    },
  ],
  VIEW: [
    {
      name: 'Simple View',
      code: `CREATE OR REPLACE VIEW emp_view AS
SELECT employee_id, first_name, salary FROM employees
WHERE salary > 50000;`,
    },
  ],
  SEQUENCE: [
    {
      name: 'Simple Seq',
      code: `CREATE SEQUENCE employees_seq START WITH 1 INCREMENT BY 1 NOCACHE;`,
    },
  ],
  INDEX: [
    {
      name: 'Simple Index',
      code: `CREATE INDEX idx_emp_name ON employees(last_name, first_name);`,
    },
  ],
};
