import { startTransition, useState, type JSX } from "react";

import { fetchPreviewPage, fetchS3Files, processFile } from "./api";
import type { ColumnInferenceResult, ProcessResponse, S3CredentialsInput, S3File } from "./types";

type ViewState = "connection" | "workbench";

const PAGE_SIZE_OPTIONS = [25, 50, 100] as const;

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

function schemaToOverrides(schema: ColumnInferenceResult[]): Record<string, string> {
  return Object.fromEntries(schema.map((column) => [column.column, column.inferred_type]));
}

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
  const [view, setView] = useState<ViewState>("connection");
  const [isWorkbenchSidebarOpen, setIsWorkbenchSidebarOpen] = useState(false);
  const [credentials, setCredentials] = useState<S3CredentialsInput>(defaultCredentials);
  const [files, setFiles] = useState<S3File[]>([]);
  const [selectedKey, setSelectedKey] = useState("");
  const [sheetName, setSheetName] = useState("");
  const [result, setResult] = useState<ProcessResponse | null>(null);
  const [detectedSchema, setDetectedSchema] = useState<ColumnInferenceResult[]>([]);
  const [overrides, setOverrides] = useState<Record<string, string>>({});
  const [rowsPerPage, setRowsPerPage] = useState<number>(25);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [busyState, setBusyState] = useState<"idle" | "listing" | "processing" | "paging">("idle");
  const [error, setError] = useState("");

  const selectedFile = files.find((item) => item.key === selectedKey) ?? null;
  const displayedSchema = detectedSchema.length > 0 ? detectedSchema : result?.schema ?? [];
  const previewRows = result?.previewRows ?? [];
  const activePage = result?.previewPage.page ?? currentPage;
  const activePageSize = result?.previewPage.pageSize ?? rowsPerPage;
  const totalPreviewRows = result?.previewPage.totalRows ?? result?.rowCount ?? 0;
  const totalPages = result?.previewPage.totalPages ?? Math.max(1, Math.ceil(totalPreviewRows / activePageSize));
  const pageStart = totalPreviewRows === 0 ? 0 : (activePage - 1) * activePageSize;
  const previewRangeStart = totalPreviewRows === 0 ? 0 : pageStart + 1;
  const previewRangeEnd = totalPreviewRows === 0 ? 0 : pageStart + previewRows.length;
  const missingConnectionFields = [
    !credentials.access_key_id && "access key ID",
    !credentials.secret_access_key && "secret access key",
    !credentials.region && "region",
    !credentials.bucket && "bucket",
  ].filter(Boolean) as string[];
  const hasConnectionDetails = missingConnectionFields.length === 0;
  const changedOverrideCount = displayedSchema.filter(
    (column) => (overrides[column.column] ?? column.inferred_type) !== column.inferred_type,
  ).length;
  const busyMessage = {
    idle: "",
    listing: "Loading supported files from S3...",
    processing: "Profiling the dataset and generating the processed preview...",
    paging: "Loading the requested preview page...",
  }[busyState];

  function resetWorkbenchState() {
    setResult(null);
    setDetectedSchema([]);
    setOverrides({});
    setCurrentPage(1);
  }

  function updateCredentialField(field: keyof S3CredentialsInput, value: string) {
    startTransition(() => {
      setCredentials((current) => ({ ...current, [field]: value }));
    });
    if (error) {
      setError("");
    }
  }

  async function handleBrowseFiles() {
    if (!hasConnectionDetails) {
      setError(`Enter ${missingConnectionFields.join(", ")} before browsing S3 files.`);
      return;
    }

    setBusyState("listing");
    setError("");
    resetWorkbenchState();

    try {
      const nextFiles = await fetchS3Files(credentials);
      setFiles(nextFiles);
      const currentSelectionStillExists = nextFiles.some((file) => file.key === selectedKey);
      if (!currentSelectionStillExists) {
        setSelectedKey(nextFiles[0]?.key ?? "");
      }
      if (nextFiles.length === 0) {
        setSelectedKey("");
        setSheetName("");
      }
      setIsWorkbenchSidebarOpen(true);
      setView("workbench");
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to load files.");
    } finally {
      setBusyState("idle");
    }
  }

  async function handleProcessFile() {
    if (!hasConnectionDetails) {
      setError(`Enter ${missingConnectionFields.join(", ")} before processing a file.`);
      return;
    }
    if (!selectedKey) {
      setError("Choose a file before processing.");
      return;
    }

    setBusyState("processing");
    setError("");

    try {
      const baselineSchema = displayedSchema;
      const baselineOverrides = schemaToOverrides(baselineSchema);
      const effectiveOverrides = Object.entries(overrides)
        .filter(([column, targetType]) => targetType !== baselineOverrides[column])
        .map(([column, target_type]) => ({ column, target_type }));
      const nextResult = await processFile({
        credentials,
        objectKey: selectedKey,
        sheetName,
        previewRowLimit: rowsPerPage,
        overrides: effectiveOverrides,
      });

      setResult(nextResult);
      setCurrentPage(nextResult.previewPage.page);
      setRowsPerPage(nextResult.previewPage.pageSize);
      if (baselineSchema.length === 0 || effectiveOverrides.length === 0) {
        setDetectedSchema(nextResult.schema);
        setOverrides(schemaToOverrides(nextResult.schema));
      } else {
        setOverrides({
          ...baselineOverrides,
          ...Object.fromEntries(effectiveOverrides.map(({ column, target_type }) => [column, target_type])),
        });
      }
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to process file.");
    } finally {
      setBusyState("idle");
    }
  }

  async function handleLoadPreviewPage(nextPage: number, nextPageSize: number = rowsPerPage) {
    if (!result) {
      return;
    }

    setBusyState("paging");
    setError("");

    try {
      const preview = await fetchPreviewPage({
        credentials,
        runId: result.runId,
        page: nextPage,
        pageSize: nextPageSize,
      });

      setResult((current) =>
        current
          ? {
              ...current,
              rowCount: preview.rowCount,
              previewColumns: preview.previewColumns,
              previewRows: preview.previewRows,
              previewPage: preview.previewPage,
            }
          : current,
      );
      setCurrentPage(preview.previewPage.page);
      setRowsPerPage(preview.previewPage.pageSize);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to load the requested preview page.");
    } finally {
      setBusyState("idle");
    }
  }

  function applySchemaDefaults() {
    setOverrides(schemaToOverrides(displayedSchema));
  }

  function handleSelectFile(file: S3File) {
    setSelectedKey(file.key);
    resetWorkbenchState();
    if (file.format !== "excel") {
      setSheetName("");
    }
    if (error) {
      setError("");
    }
  }

  function handleEditConnection() {
    setIsWorkbenchSidebarOpen(false);
    setView("connection");
    if (error) {
      setError("");
    }
  }

  async function handleRowsPerPageChange(value: number) {
    if (!result) {
      setRowsPerPage(value);
      setCurrentPage(1);
      return;
    }

    await handleLoadPreviewPage(1, value);
  }

  async function goToNextPage() {
    if (!result?.previewPage.hasNextPage) {
      return;
    }

    await handleLoadPreviewPage(activePage + 1, activePageSize);
  }

  async function goToPreviousPage() {
    if (!result?.previewPage.hasPreviousPage) {
      return;
    }

    await handleLoadPreviewPage(activePage - 1, activePageSize);
  }

  return (
    <main className="page-shell">
      {busyState !== "idle" ? (
        <div className="loading-panel" role="status" aria-live="polite">
          <div className="loading-bar" aria-hidden="true">
            <span />
          </div>
          <p>{busyMessage}</p>
        </div>
      ) : null}

      {view === "connection" ? (
        <>
          <section className="hero-panel">
            <p className="eyebrow">Rhombus AI Home Test</p>
            <h1>S3 data type inference workbench</h1>
            <p className="hero-copy">
              Connect to S3, select a CSV or Excel file, profile the schema with stricter inference rules, and manually
              override any column before previewing the processed data.
            </p>
          </section>

          {error ? <div className="callout danger">{error}</div> : null}

          <section className="connection-layout">
            <article className="card">
              <div className="card-header">
                <div>
                  <p className="section-label">Step 1</p>
                  <h2>S3 connection</h2>
                </div>
                <button
                  className="primary-button"
                  onClick={handleBrowseFiles}
                  disabled={busyState !== "idle" || !hasConnectionDetails}
                >
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
              {!hasConnectionDetails ? (
                <p className="helper-text">Required before browsing: {missingConnectionFields.join(", ")}.</p>
              ) : files.length > 0 ? (
                <p className="helper-text">Browsing again will refresh the current workbench file list and preview state.</p>
              ) : null}
            </article>
          </section>
        </>
      ) : (
        <>
          <section className="workbench-header">
            <div>
              <p className="eyebrow">Rhombus AI Home Test</p>
              <h1>Processing workbench</h1>
              <p className="hero-copy">
                Connected to <strong>{credentials.bucket || "No bucket selected"}</strong> in <strong>{credentials.region}</strong>.
              </p>
            </div>
            <div className="workbench-header-actions">
              <div className="workbench-summary">
                <span>{files.length} supported files</span>
                <span>{selectedFile?.key ?? "No file selected"}</span>
              </div>
              <div className="workbench-header-buttons">
                <button
                  className="secondary-button"
                  onClick={() => setIsWorkbenchSidebarOpen((open) => !open)}
                >
                  {isWorkbenchSidebarOpen ? "Hide files & schema" : "Open files & schema"}
                </button>
                <button className="secondary-button" onClick={handleEditConnection}>
                  Edit connection
                </button>
              </div>
            </div>
          </section>

          {error ? <div className="callout danger">{error}</div> : null}

          <section className="workbench-stage">
            <section className="workbench-main">
              <article className="card preview-card">
                <div className="card-header compact">
                  <div>
                    <p className="section-label">Step 4</p>
                    <h2>Processed preview</h2>
                  </div>
                  {result ? (
                    <label className="pagination-select">
                      Rows per page
                      <select
                        aria-label="Rows per page"
                        value={rowsPerPage}
                        onChange={(event) => {
                          void handleRowsPerPageChange(Number(event.target.value));
                        }}
                        disabled={busyState !== "idle"}
                      >
                        {PAGE_SIZE_OPTIONS.map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                    </label>
                  ) : null}
                </div>

                {result ? (
                  <>
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

                    <div className="pagination-bar">
                      <span className="pagination-status">
                        Preview rows {previewRangeStart}-{previewRangeEnd} of {totalPreviewRows}
                      </span>
                      <div className="pagination-controls">
                        <button
                          className="secondary-button pagination-button"
                          aria-label="Previous page"
                          onClick={() => {
                            void goToPreviousPage();
                          }}
                          disabled={!result.previewPage.hasPreviousPage || busyState !== "idle"}
                        >
                          Previous
                        </button>
                        <span className="page-indicator">
                          Page {activePage} of {totalPages}
                        </span>
                        <button
                          className="secondary-button pagination-button"
                          aria-label="Next page"
                          onClick={() => {
                            void goToNextPage();
                          }}
                          disabled={!result.previewPage.hasNextPage || busyState !== "idle"}
                        >
                          Next
                        </button>
                      </div>
                    </div>

                    <div className="table-wrap preview-table-wrap">
                      <table>
                        <thead>
                          <tr>
                            {result.previewColumns.map((column) => (
                              <th key={column}>{column}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {previewRows.map((row, index) => (
                            <tr key={`row-${activePage}-${index}`}>
                              {result.previewColumns.map((column) => (
                                <td key={`${activePage}-${index}-${column}`}>{String(row[column] ?? "")}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <div className="empty-state">
                    <p>
                      {files.length === 0
                        ? "Browse a different bucket or prefix to load supported files into the workbench."
                        : "Select a file and process it to preview converted rows here."}
                    </p>
                  </div>
                )}
              </article>
            </section>

            {isWorkbenchSidebarOpen ? (
              <>
                <button
                  className="workbench-drawer-backdrop"
                  aria-label="Close files and schema panel"
                  onClick={() => setIsWorkbenchSidebarOpen(false)}
                />
                <aside className="workbench-drawer" aria-label="Files and schema panel">
                  <div className="drawer-header">
                    <div>
                      <p className="eyebrow">Workbench controls</p>
                      <h2>Files and schema</h2>
                    </div>
                    <button
                      className="secondary-button drawer-close-button"
                      onClick={() => setIsWorkbenchSidebarOpen(false)}
                    >
                      Close panel
                    </button>
                  </div>

                  <div className="drawer-grid">
                    <article className="card drawer-card">
                      <div className="card-header stacked">
                        <div>
                          <p className="section-label">Step 2</p>
                          <h2>Supported files</h2>
                        </div>
                        <div className="card-actions">
                          <button
                            className="primary-button"
                            onClick={handleProcessFile}
                            disabled={!selectedKey || busyState !== "idle"}
                          >
                            {busyState === "processing" ? "Processing..." : "Process selection"}
                          </button>
                        </div>
                      </div>

                      <div className="metrics-row">
                        <div className="metric">
                          <span className="metric-label">Supported objects</span>
                          <strong>{files.length}</strong>
                        </div>
                        <div className="metric">
                          <span className="metric-label">Selected file</span>
                          <strong>{selectedFile?.key ?? "None"}</strong>
                        </div>
                      </div>

                      {files.length === 0 ? (
                        <div className="empty-state">
                          <p>No supported files were found for this bucket or prefix.</p>
                        </div>
                      ) : (
                        <div className="file-list">
                          {files.map((file) => (
                            <button
                              key={file.key}
                              className={`file-item ${file.key === selectedKey ? "selected" : ""}`}
                              onClick={() => handleSelectFile(file)}
                            >
                              <span>{file.key}</span>
                              <span>{file.format.toUpperCase()} - {formatBytes(file.size)}</span>
                            </button>
                          ))}
                        </div>
                      )}

                      {selectedFile?.format === "excel" ? (
                        <label>
                          Optional sheet name
                          <input
                            value={sheetName}
                            onChange={(event) => setSheetName(event.target.value)}
                            placeholder="First sheet if blank"
                          />
                        </label>
                      ) : null}
                    </article>

                    <article className="card drawer-card schema-card">
                      <div className="card-header stacked compact">
                        <div>
                          <p className="section-label">Step 3</p>
                          <h2>Inferred schema</h2>
                        </div>
                        <div className="card-actions">
                          <button
                            className="secondary-button"
                            onClick={applySchemaDefaults}
                            disabled={busyState !== "idle" || displayedSchema.length === 0}
                          >
                            Reset overrides
                          </button>
                        </div>
                      </div>

                      {result ? (
                        <>
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
                            <div className="metric">
                              <span className="metric-label">Changed overrides</span>
                              <strong>{changedOverrideCount}</strong>
                            </div>
                          </div>

                          <div className="table-wrap schema-table-wrap">
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
                                {displayedSchema.map((column) => (
                                  <tr key={column.column}>
                                    <td>{column.column}</td>
                                    <td>{column.display_type}</td>
                                    <td>
                                      <select
                                        aria-label={`Override type for ${column.column}`}
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
                            <button className="secondary-button" onClick={handleProcessFile} disabled={busyState !== "idle"}>
                              Reprocess with overrides
                            </button>
                          </div>
                        </>
                      ) : (
                        <div className="empty-state">
                          <p>Process the selected file to infer its schema and manage column overrides.</p>
                        </div>
                      )}
                    </article>
                  </div>
                </aside>
              </>
            ) : null}
          </section>
        </>
      )}
    </main>
  );
}
