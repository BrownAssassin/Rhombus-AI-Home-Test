import { startTransition, useState } from "react";

import { fetchS3Files, processFile } from "./api";
import type { ColumnInferenceResult, ProcessResponse, S3CredentialsInput, S3File } from "./types";


const defaultCredentials: S3CredentialsInput = {
  access_key_id: "",
  secret_access_key: "",
  session_token: "",
  region: "ap-southeast-2",
  bucket: "",
  prefix: "",
};

const typeLabelOverrides: Record<string, string> = {
  text: "Text",
  integer: "Integer",
  float: "Float",
  boolean: "Boolean",
  date: "Date",
  datetime: "DateTime",
  category: "Category",
  complex: "Complex",
};


function formatBytes(value: number): string {
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}


function formatColumnWarnings(schema: ColumnInferenceResult[]): JSX.Element | null {
  const warningEntries = schema.flatMap((item) => item.warnings.map((warning) => `${item.column}: ${warning}`));
  if (warningEntries.length === 0) {
    return null;
  }

  return (
    <div className="callout warning">
      <h3>Warnings</h3>
      <ul>
        {warningEntries.map((warning) => (
          <li key={warning}>{warning}</li>
        ))}
      </ul>
    </div>
  );
}


export default function App() {
  const [credentials, setCredentials] = useState<S3CredentialsInput>(defaultCredentials);
  const [files, setFiles] = useState<S3File[]>([]);
  const [selectedKey, setSelectedKey] = useState("");
  const [sheetName, setSheetName] = useState("");
  const [result, setResult] = useState<ProcessResponse | null>(null);
  const [overrides, setOverrides] = useState<Record<string, string>>({});
  const [busyState, setBusyState] = useState<"idle" | "listing" | "processing">("idle");
  const [error, setError] = useState("");

  const selectedFile = files.find((item) => item.key === selectedKey) ?? null;

  function updateCredentialField(field: keyof S3CredentialsInput, value: string) {
    startTransition(() => {
      setCredentials((current) => ({ ...current, [field]: value }));
    });
  }

  async function handleBrowseFiles() {
    setBusyState("listing");
    setError("");
    setResult(null);
    try {
      const nextFiles = await fetchS3Files(credentials);
      setFiles(nextFiles);
      if (nextFiles.length > 0 && !selectedKey) {
        setSelectedKey(nextFiles[0].key);
      }
      if (nextFiles.length === 0) {
        setSelectedKey("");
      }
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to load files.");
    } finally {
      setBusyState("idle");
    }
  }

  async function handleProcessFile() {
    if (!selectedKey) {
      setError("Choose a file before processing.");
      return;
    }

    setBusyState("processing");
    setError("");

    try {
      const nextResult = await processFile({
        credentials,
        objectKey: selectedKey,
        sheetName,
        previewRowLimit: 100,
        overrides: Object.entries(overrides).map(([column, target_type]) => ({ column, target_type })),
      });
      setResult(nextResult);
      setOverrides(
        Object.fromEntries(nextResult.schema.map((column) => [column.column, column.inferred_type])),
      );
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to process file.");
    } finally {
      setBusyState("idle");
    }
  }

  function applySchemaDefaults(schema: ColumnInferenceResult[]) {
    setOverrides(Object.fromEntries(schema.map((column) => [column.column, column.inferred_type])));
  }

  return (
    <main className="page-shell">
      <section className="hero-panel">
        <p className="eyebrow">Rhombus AI Home Test</p>
        <h1>S3 data type inference workbench</h1>
        <p className="hero-copy">
          Connect to S3, select a CSV or Excel file, profile the schema with stricter inference rules, and
          manually override any column before previewing the processed data.
        </p>
      </section>

      <section className="card-grid">
        <article className="card">
          <div className="card-header">
            <div>
              <p className="section-label">Step 1</p>
              <h2>S3 connection</h2>
            </div>
            <button className="primary-button" onClick={handleBrowseFiles} disabled={busyState !== "idle"}>
              {busyState === "listing" ? "Loading files..." : "Browse files"}
            </button>
          </div>
          <div className="field-grid">
            <label>
              Access key ID
              <input
                value={credentials.access_key_id}
                onChange={(event) => updateCredentialField("access_key_id", event.target.value)}
                placeholder="AKIA..."
              />
            </label>
            <label>
              Secret access key
              <input
                type="password"
                value={credentials.secret_access_key}
                onChange={(event) => updateCredentialField("secret_access_key", event.target.value)}
                placeholder="AWS secret"
              />
            </label>
            <label>
              Session token
              <input
                value={credentials.session_token}
                onChange={(event) => updateCredentialField("session_token", event.target.value)}
                placeholder="Optional temporary token"
              />
            </label>
            <label>
              Region
              <input
                value={credentials.region}
                onChange={(event) => updateCredentialField("region", event.target.value)}
                placeholder="ap-southeast-2"
              />
            </label>
            <label>
              Bucket
              <input
                value={credentials.bucket}
                onChange={(event) => updateCredentialField("bucket", event.target.value)}
                placeholder="my-data-bucket"
              />
            </label>
            <label>
              Prefix
              <input
                value={credentials.prefix}
                onChange={(event) => updateCredentialField("prefix", event.target.value)}
                placeholder="optional/folder/"
              />
            </label>
          </div>
          <p className="helper-text">
            Credentials stay in component state only. They are never saved to local storage by this app.
          </p>
        </article>

        <article className="card">
          <div className="card-header">
            <div>
              <p className="section-label">Step 2</p>
              <h2>Supported files</h2>
            </div>
            <button className="secondary-button" onClick={handleProcessFile} disabled={!selectedKey || busyState !== "idle"}>
              {busyState === "processing" ? "Processing..." : "Process selection"}
            </button>
          </div>

          {files.length === 0 ? (
            <div className="empty-state">
              <p>No files loaded yet. Browse the bucket to list `.csv`, `.xls`, and `.xlsx` objects.</p>
            </div>
          ) : (
            <>
              <div className="file-list">
                {files.map((file) => (
                  <button
                    key={file.key}
                    className={`file-item ${file.key === selectedKey ? "selected" : ""}`}
                    onClick={() => setSelectedKey(file.key)}
                  >
                    <span>{file.key}</span>
                    <span>{file.format.toUpperCase()} · {formatBytes(file.size)}</span>
                  </button>
                ))}
              </div>
              {selectedFile?.format === "excel" ? (
                <label>
                  Optional sheet name
                  <input value={sheetName} onChange={(event) => setSheetName(event.target.value)} placeholder="First sheet if blank" />
                </label>
              ) : null}
            </>
          )}
        </article>
      </section>

      {error ? <div className="callout danger">{error}</div> : null}

      {result ? (
        <>
          <section className="results-grid">
            <article className="card">
              <div className="card-header compact">
                <div>
                  <p className="section-label">Step 3</p>
                  <h2>Inferred schema</h2>
                </div>
                <button
                  className="secondary-button"
                  onClick={() => applySchemaDefaults(result.schema)}
                  disabled={busyState !== "idle"}
                >
                  Reset overrides
                </button>
              </div>
              <div className="metrics-row">
                <div className="metric">
                  <span className="metric-label">Rows profiled</span>
                  <strong>{result.rowCount.toLocaleString()}</strong>
                </div>
                <div className="metric">
                  <span className="metric-label">Processed in</span>
                  <strong>{result.processingMetadata.durationMs} ms</strong>
                </div>
                <div className="metric">
                  <span className="metric-label">Run ID</span>
                  <strong>{result.runId}</strong>
                </div>
              </div>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Column</th>
                      <th>Detected</th>
                      <th>Override</th>
                      <th>Confidence</th>
                      <th>Samples</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.schema.map((column) => (
                      <tr key={column.column}>
                        <td>{column.column}</td>
                        <td>{column.display_type}</td>
                        <td>
                          <select
                            value={overrides[column.column] ?? column.inferred_type}
                            onChange={(event) =>
                              setOverrides((current) => ({
                                ...current,
                                [column.column]: event.target.value,
                              }))
                            }
                          >
                            {column.allowed_overrides.map((option) => (
                              <option key={option} value={option}>
                                {typeLabelOverrides[option] ?? option}
                              </option>
                            ))}
                          </select>
                        </td>
                        <td>{Math.round(column.confidence * 100)}%</td>
                        <td>{column.sample_values.join(", ") || "No non-null sample"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="toolbar">
                <button className="primary-button" onClick={handleProcessFile} disabled={busyState !== "idle"}>
                  Reprocess with overrides
                </button>
              </div>
            </article>

            <article className="card">
              <div className="card-header compact">
                <div>
                  <p className="section-label">Step 4</p>
                  <h2>Processed preview</h2>
                </div>
              </div>
              {result.warnings.length > 0 ? (
                <div className="callout warning">
                  <h3>Dataset warnings</h3>
                  <ul>
                    {result.warnings.map((warning) => (
                      <li key={warning}>{warning}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
              {formatColumnWarnings(result.schema)}
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      {result.previewColumns.map((column) => (
                        <th key={column}>{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.previewRows.map((row, index) => (
                      <tr key={`row-${index}`}>
                        {result.previewColumns.map((column) => (
                          <td key={`${index}-${column}`}>{String(row[column] ?? "")}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>
          </section>
        </>
      ) : null}
    </main>
  );
}

