import { startTransition, useEffect, useState, type JSX } from "react";

import {
  ApiError,
  fetchPreviewPage,
  fetchRecentRuns,
  fetchRunStatus,
  fetchS3Files,
  processFile,
  processFileAsync,
  runSparkComparison,
} from "./api";
import type {
  ColumnInferenceResult,
  ProcessResponse,
  RunStatusResponse,
  RunSummary,
  S3CredentialsInput,
  S3File,
  SparkComparisonResponse,
} from "./types";

type ViewState = "connection" | "workbench";
type BusyState = "idle" | "listing" | "processing" | "queueing" | "paging" | "spark" | "loadingRun";

const PAGE_SIZE_OPTIONS = [25, 50, 100] as const;
const RECENT_RUN_LIMIT = 6;

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

function isRunActive(run: RunSummary): boolean {
  return run.status === "queued" || run.status === "processing";
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

function formatRunType(runType: RunSummary["runType"]): string {
  return runType === "spark_compare" ? "Spark comparison" : "Process";
}

function formatRunMoment(run: RunSummary): string {
  const timestamp = run.completedAt ?? run.startedAt ?? run.createdAt;
  if (!timestamp) {
    return "Just now";
  }
  return new Date(timestamp).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
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

function LoadingNotice({ message }: { message: string }): JSX.Element {
  return (
    <div className="loading-panel" role="status" aria-live="polite">
      <div className="loading-bar" aria-hidden="true">
        <span />
      </div>
      <p>{message}</p>
    </div>
  );
}

function buildProcessResponseFromRun(run: RunStatusResponse): ProcessResponse | null {
  if (
    run.status !== "completed" ||
    run.runType !== "process" ||
    !run.schema ||
    !run.previewColumns ||
    !run.previewRows ||
    !run.previewPage ||
    !run.processingMetadata ||
    typeof run.rowCount !== "number" ||
    !run.fileType
  ) {
    return null;
  }

  return {
    runId: run.runId,
    rowCount: run.rowCount,
    schema: run.schema,
    previewColumns: run.previewColumns,
    previewRows: run.previewRows,
    previewPage: run.previewPage,
    warnings: run.warnings ?? [],
    processingMetadata: {
      durationMs: run.processingMetadata.durationMs,
      previewRowLimit: run.processingMetadata.previewRowLimit ?? 100,
      chunkSize: run.processingMetadata.chunkSize ?? null,
      appliedOverrides: run.processingMetadata.appliedOverrides ?? {},
    },
    selectedSheet: run.selectedSheet ?? "",
    fileType: run.fileType,
  };
}

function buildQueuedRunSummary(args: {
  runId: number;
  taskId: string;
  runType: "process" | "spark_compare";
  sourceRunId?: number | null;
  engine: "pandas" | "spark";
  bucket: string;
  objectKey: string;
  fileType?: "csv" | "excel";
  selectedSheet?: string;
}): RunSummary {
  const now = new Date().toISOString();
  return {
    runId: args.runId,
    taskId: args.taskId,
    runType: args.runType,
    sourceRunId: args.sourceRunId ?? null,
    status: "queued",
    engine: args.engine,
    bucket: args.bucket,
    objectKey: args.objectKey,
    progressStage: "queued",
    progressPercent: 0,
    errorMessage: "",
    createdAt: now,
    startedAt: null,
    completedAt: null,
    fileType: args.fileType,
    selectedSheet: args.selectedSheet ?? "",
  };
}

function buildEffectiveOverrides(
  schema: ColumnInferenceResult[],
  overrides: Record<string, string>,
): {
  rows: Array<{ column: string; target_type: string }>;
  appliedOverrideMap: Record<string, string>;
} {
  const baselineOverrides = schemaToOverrides(schema);
  const rows = Object.entries(overrides)
    .filter(([column, targetType]) => targetType !== baselineOverrides[column])
    .map(([column, target_type]) => ({ column, target_type }));

  return {
    rows,
    appliedOverrideMap: Object.fromEntries(rows.map(({ column, target_type }) => [column, target_type])),
  };
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
  const [busyState, setBusyState] = useState<BusyState>("idle");
  const [recentRuns, setRecentRuns] = useState<RunSummary[]>([]);
  const [pendingProcessRunId, setPendingProcessRunId] = useState<number | null>(null);
  const [sparkComparison, setSparkComparison] = useState<SparkComparisonResponse | null>(null);
  const [error, setError] = useState("");
  const [fallbackNotice, setFallbackNotice] = useState("");

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
  const runIsActive = recentRuns.some(isRunActive);
  const latestActiveRun = recentRuns.find(isRunActive) ?? null;
  const busyMessage = {
    idle: "",
    listing: "Loading supported files from S3...",
    processing: "Profiling the dataset and generating the processed preview...",
    queueing: "Starting a tracked background job...",
    paging: "Loading the requested preview page...",
    spark: "Running the experimental Spark comparison...",
    loadingRun: "Loading the selected run result...",
  }[busyState];

  useEffect(() => {
    if (view !== "workbench" || !selectedKey) {
      setRecentRuns([]);
      return undefined;
    }

    let isCancelled = false;

    const loadRuns = async () => {
      try {
        const nextRuns = await fetchRecentRuns({ objectKey: selectedKey, limit: RECENT_RUN_LIMIT });
        if (!isCancelled) {
          setRecentRuns(nextRuns);
        }
      } catch (caughtError) {
        if (!isCancelled) {
          setError(caughtError instanceof Error ? caughtError.message : "Unable to load the recent job list.");
        }
      }
    };

    void loadRuns();

    return () => {
      isCancelled = true;
    };
  }, [selectedKey, view]);

  useEffect(() => {
    if (view !== "workbench" || !selectedKey || !runIsActive) {
      return undefined;
    }

    let isCancelled = false;
    let timeoutId: number | undefined;

    const pollRuns = async () => {
      let nextRuns: RunSummary[] = [];

      try {
        nextRuns = await fetchRecentRuns({ objectKey: selectedKey, limit: RECENT_RUN_LIMIT });
        if (isCancelled) {
          return;
        }

        setRecentRuns(nextRuns);

        if (pendingProcessRunId !== null) {
          const pendingRun = nextRuns.find((run) => run.runId === pendingProcessRunId);
          if (pendingRun?.status === "completed") {
            const nextRun = await fetchRunStatus(pendingRun.runId);
            if (isCancelled) {
              return;
            }
            const nextResult = buildProcessResponseFromRun(nextRun);
            if (nextResult) {
              applyProcessResult(nextResult, nextResult.processingMetadata.appliedOverrides ?? {});
            }
            setPendingProcessRunId(null);
          } else if (pendingRun?.status === "failed") {
            setError(pendingRun.errorMessage || "The background processing job failed.");
            setPendingProcessRunId(null);
          }
        }
      } catch (caughtError) {
        if (!isCancelled) {
          setError(caughtError instanceof Error ? caughtError.message : "Unable to refresh the recent job list.");
        }
      } finally {
        if (!isCancelled && nextRuns.some(isRunActive)) {
          const nextInterval = nextRuns.some((run) => run.status === "queued") ? 1200 : 2500;
          timeoutId = window.setTimeout(() => {
            void pollRuns();
          }, nextInterval);
        }
      }
    };

    void pollRuns();

    return () => {
      isCancelled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [pendingProcessRunId, runIsActive, selectedKey, view]);

  function applyProcessResult(nextResult: ProcessResponse, appliedOverrides: Record<string, string>) {
    setResult(nextResult);
    setCurrentPage(nextResult.previewPage.page);
    setRowsPerPage(nextResult.previewPage.pageSize);
    setDetectedSchema(nextResult.schema);
    setOverrides({
      ...schemaToOverrides(nextResult.schema),
      ...appliedOverrides,
    });
    setSparkComparison(null);
  }

  function resetWorkbenchState() {
    setResult(null);
    setDetectedSchema([]);
    setOverrides({});
    setCurrentPage(1);
    setRecentRuns([]);
    setPendingProcessRunId(null);
    setSparkComparison(null);
    setFallbackNotice("");
  }

  function updateCredentialField(field: keyof S3CredentialsInput, value: string) {
    startTransition(() => {
      setCredentials((current) => ({ ...current, [field]: value }));
    });
    if (error) {
      setError("");
    }
  }

  function upsertQueuedRun(queuedRun: RunSummary) {
    setRecentRuns((current) => [queuedRun, ...current.filter((run) => run.runId !== queuedRun.runId)].slice(0, RECENT_RUN_LIMIT));
  }

  async function handleBrowseFiles() {
    if (!hasConnectionDetails) {
      setError(`Enter ${missingConnectionFields.join(", ")} before browsing S3 files.`);
      return;
    }

    setBusyState("listing");
    setError("");
    setFallbackNotice("");
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

  async function refreshRecentRuns(suppressErrors = false) {
    if (!selectedKey) {
      setRecentRuns([]);
      return [];
    }

    try {
      const nextRuns = await fetchRecentRuns({ objectKey: selectedKey, limit: RECENT_RUN_LIMIT });
      setRecentRuns(nextRuns);
      return nextRuns;
    } catch (caughtError) {
      if (!suppressErrors) {
        setError(caughtError instanceof Error ? caughtError.message : "Unable to refresh the recent job list.");
      }
      return [];
    }
  }

  async function runSynchronousProcess(appliedOverrideMap: Record<string, string>) {
    setBusyState("processing");

    try {
      const nextResult = await processFile({
        credentials,
        objectKey: selectedKey,
        sheetName,
        previewRowLimit: rowsPerPage,
        overrides: Object.entries(appliedOverrideMap).map(([column, target_type]) => ({ column, target_type })),
      });

      applyProcessResult(nextResult, appliedOverrideMap);
      setPendingProcessRunId(null);
      await refreshRecentRuns(true);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to process file.");
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

    setBusyState("queueing");
    setError("");
    setFallbackNotice("");
    setSparkComparison(null);

    const { rows: effectiveOverrides, appliedOverrideMap } = buildEffectiveOverrides(displayedSchema, overrides);

    try {
      const queuedRun = await processFileAsync({
        credentials,
        objectKey: selectedKey,
        sheetName,
        previewRowLimit: rowsPerPage,
        overrides: effectiveOverrides,
      });

      setPendingProcessRunId(queuedRun.runId);
      upsertQueuedRun(
        buildQueuedRunSummary({
          ...queuedRun,
          bucket: credentials.bucket,
          objectKey: selectedKey,
          fileType: selectedFile?.format,
          selectedSheet: sheetName,
        }),
      );
      await refreshRecentRuns(true);
    } catch (caughtError) {
      if (caughtError instanceof ApiError && (caughtError.code === "task_queue_error" || caughtError.status === 503)) {
        setFallbackNotice("Background queueing was unavailable, so the app processed this file inline instead.");
        await runSynchronousProcess(appliedOverrideMap);
        return;
      }

      setError(caughtError instanceof Error ? caughtError.message : "Unable to queue background processing.");
    } finally {
      if (busyState !== "processing") {
        setBusyState("idle");
      }
    }
  }

  async function handleRunSparkComparison() {
    if (!hasConnectionDetails) {
      setError(`Enter ${missingConnectionFields.join(", ")} before running the Spark comparison.`);
      return;
    }
    if (!selectedKey) {
      setError("Choose a CSV file before running the Spark comparison.");
      return;
    }

    setBusyState("spark");
    setError("");

    try {
      const nextComparison = await runSparkComparison({
        credentials,
        objectKey: selectedKey,
        page: 1,
        pageSize: rowsPerPage,
      });
      setSparkComparison(nextComparison);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to run the Spark comparison.");
    } finally {
      setBusyState("idle");
    }
  }

  async function handleLoadPreviewPage(nextPage: number, nextPageSize: number = rowsPerPage) {
    if (!result || !selectedKey) {
      return;
    }

    setBusyState("paging");
    setError("");

    try {
      const preview = await fetchPreviewPage({
        credentials,
        runId: result.runId,
        objectKey: selectedKey,
        fileType: result.fileType,
        selectedSheet: result.selectedSheet,
        rowCount: result.rowCount,
        schema: result.schema,
        previewColumns: result.previewColumns,
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

  async function handleViewProcessRun(runId: number) {
    setBusyState("loadingRun");
    setError("");

    try {
      const run = await fetchRunStatus(runId);
      const nextResult = buildProcessResponseFromRun(run);
      if (!nextResult) {
        throw new Error("The selected run is not a completed Pandas processing result.");
      }
      applyProcessResult(nextResult, nextResult.processingMetadata.appliedOverrides ?? {});
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to load the selected run result.");
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
              {busyState === "listing" ? <LoadingNotice message={busyMessage} /> : null}
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
                <button className="secondary-button" onClick={() => setIsWorkbenchSidebarOpen((open) => !open)}>
                  {isWorkbenchSidebarOpen ? "Hide files & schema" : "Open files & schema"}
                </button>
                <button className="secondary-button" onClick={handleEditConnection}>
                  Edit connection
                </button>
              </div>
            </div>
          </section>

          {error ? <div className="callout danger">{error}</div> : null}
          {fallbackNotice ? <div className="callout warning">{fallbackNotice}</div> : null}

          <section className="workbench-stage">
            <section className="workbench-main">
              {recentRuns.length > 0 ? (
                <section className="card jobs-tray">
                  <div className="card-header compact">
                    <div>
                      <p className="section-label">Recent jobs</p>
                      <h2>Tracked processing</h2>
                    </div>
                  </div>

                  {latestActiveRun ? (
                    <LoadingNotice
                      message={`${formatRunType(latestActiveRun.runType)} for ${latestActiveRun.objectKey} is ${latestActiveRun.progressStage || latestActiveRun.status}.`}
                    />
                  ) : null}

                  <div className="job-list">
                    {recentRuns.map((run) => {
                      const isCurrentResult = result?.runId === run.runId;
                      const canRetry = run.runType === "process" && run.status === "failed" && run.objectKey === selectedKey;
                      const canViewResult = run.runType === "process" && run.status === "completed";

                      return (
                        <article key={run.runId} className={`job-card ${isCurrentResult ? "job-card-current" : ""}`}>
                          <div className="job-card-header">
                            <div>
                              <p className="job-title">{run.objectKey}</p>
                              <p className="job-meta">
                                {formatRunType(run.runType)} · {run.engine} · {formatRunMoment(run)}
                              </p>
                            </div>
                            <span className={`job-status job-status-${run.status}`}>{run.status}</span>
                          </div>
                          <div className="job-progress-row">
                            <span>{run.progressStage || run.status}</span>
                            <strong>{run.progressPercent}%</strong>
                          </div>
                          {run.errorMessage ? <p className="job-error">{run.errorMessage}</p> : null}
                          <div className="job-actions">
                            {canViewResult ? (
                              <button
                                className="secondary-button"
                                onClick={() => {
                                  void handleViewProcessRun(run.runId);
                                }}
                                disabled={busyState !== "idle" || isCurrentResult}
                              >
                                {isCurrentResult ? "Viewing result" : "View result"}
                              </button>
                            ) : null}
                            {canRetry ? (
                              <button className="secondary-button" onClick={handleProcessFile} disabled={busyState !== "idle"}>
                                Retry
                              </button>
                            ) : null}
                          </div>
                        </article>
                      );
                    })}
                  </div>
                </section>
              ) : null}

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
                        disabled={busyState === "paging" || busyState === "loadingRun"}
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

                {busyState === "paging" || busyState === "loadingRun" ? <LoadingNotice message={busyMessage} /> : null}

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
                          disabled={!result.previewPage.hasPreviousPage || busyState === "paging" || busyState === "loadingRun"}
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
                          disabled={!result.previewPage.hasNextPage || busyState === "paging" || busyState === "loadingRun"}
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

                {sparkComparison ? (
                  <section className="comparison-panel">
                    <div className="card-header compact">
                      <div>
                        <p className="section-label">Experimental</p>
                        <h2>Spark comparison</h2>
                      </div>
                    </div>
                    <div className="metrics-row">
                      <div className="metric">
                        <span className="metric-label">Rows counted</span>
                        <strong>{sparkComparison.rowCount.toLocaleString()}</strong>
                      </div>
                      <div className="metric">
                        <span className="metric-label">Spark runtime</span>
                        <strong>{sparkComparison.processingMetadata.durationMs} ms</strong>
                      </div>
                      <div className="metric">
                        <span className="metric-label">Spark master</span>
                        <strong>{sparkComparison.processingMetadata.sparkMaster}</strong>
                      </div>
                    </div>
                    <div className="callout warning">
                      <h3>Comparison note</h3>
                      <ul>
                        {sparkComparison.notes.map((note) => (
                          <li key={note}>{note}</li>
                        ))}
                      </ul>
                    </div>
                    <div className="comparison-grid">
                      <div className="table-wrap comparison-table-wrap">
                        <table>
                          <thead>
                            <tr>
                              <th>Column</th>
                              <th>Spark type</th>
                              <th>Mapped type</th>
                              <th>Nullable</th>
                            </tr>
                          </thead>
                          <tbody>
                            {sparkComparison.sparkSchema.map((item) => (
                              <tr key={item.column}>
                                <td>{item.column}</td>
                                <td>{item.sparkType}</td>
                                <td>{item.displayType}</td>
                                <td>{item.nullable ? "Yes" : "No"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <div className="table-wrap comparison-table-wrap">
                        <table>
                          <thead>
                            <tr>
                              {sparkComparison.previewColumns.map((column) => (
                                <th key={column}>{column}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {sparkComparison.previewRows.map((row, index) => (
                              <tr key={`spark-row-${index}`}>
                                {sparkComparison.previewColumns.map((column) => (
                                  <td key={`spark-${index}-${column}`}>{String(row[column] ?? "")}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </section>
                ) : null}
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
                            disabled={!selectedKey || busyState !== "idle" || runIsActive}
                          >
                            {busyState === "queueing"
                              ? "Starting job..."
                              : busyState === "processing"
                                ? "Processing..."
                                : "Process file"}
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

                      {busyState === "listing" || busyState === "queueing" || busyState === "processing" || busyState === "spark" ? (
                        <LoadingNotice message={busyMessage} />
                      ) : null}

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
                              <span>
                                {file.format.toUpperCase()} - {formatBytes(file.size)}
                              </span>
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

                      <section className="advanced-section">
                        <div>
                          <p className="section-label">Advanced processing</p>
                          <h3>Comparison tools</h3>
                        </div>
                        <div className="advanced-actions">
                          <button
                            className="secondary-button"
                            onClick={handleRunSparkComparison}
                            disabled={!selectedKey || selectedFile?.format !== "csv" || busyState !== "idle" || runIsActive}
                          >
                            Run Spark comparison
                          </button>
                        </div>
                        <p className="helper-text">
                          Processing now prefers tracked background jobs automatically. Spark comparison remains experimental and CSV-only.
                        </p>
                      </section>
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
                            disabled={busyState === "processing" || busyState === "queueing" || displayedSchema.length === 0}
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
                                        disabled={busyState === "processing" || busyState === "queueing"}
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
                            <button className="secondary-button" onClick={handleProcessFile} disabled={busyState !== "idle" || runIsActive}>
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
