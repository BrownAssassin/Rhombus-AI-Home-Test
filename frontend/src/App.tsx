import { startTransition, useEffect, useRef, useState, type JSX } from "react";

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
type WorkbenchPanel = "files" | "schema" | null;
type InspectorMode = "schema" | "spark";
type ResizingPanel = Exclude<WorkbenchPanel, null>;

const PAGE_SIZE_OPTIONS = [25, 50, 100] as const;
const RECENT_RUN_LIMIT = 6;
const DESKTOP_PANEL_BREAKPOINT = 960;
const FILES_PANEL_STORAGE_KEY = "rhombus-workbench-files-panel-width";
const SCHEMA_PANEL_STORAGE_KEY = "rhombus-workbench-schema-panel-width";
const FILES_PANEL_DEFAULT_WIDTH = 512;
const FILES_PANEL_MIN_WIDTH = 440;
const FILES_PANEL_MAX_WIDTH = 640;
const SCHEMA_PANEL_MIN_WIDTH = 860;

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

function formatFileFormat(file: S3File | null): string {
  if (!file) {
    return "No file selected";
  }
  return file.format === "excel" ? "Excel workbook" : "CSV dataset";
}

function formatFileTimestamp(timestamp: string | null): string {
  if (!timestamp) {
    return "Unknown update time";
  }
  return new Date(timestamp).toLocaleDateString([], {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatJobMeta(run: RunSummary): string {
  return `${formatRunType(run.runType)} | ${run.engine} | ${formatRunMoment(run)}`;
}

function isDesktopViewport(): boolean {
  if (typeof window === "undefined") {
    return true;
  }

  return window.innerWidth > DESKTOP_PANEL_BREAKPOINT;
}

function clampFilesPanelWidth(width: number, viewportWidth: number): number {
  const maxWidth = Math.min(FILES_PANEL_MAX_WIDTH, Math.max(FILES_PANEL_MIN_WIDTH, viewportWidth - 48));
  return Math.min(Math.max(width, FILES_PANEL_MIN_WIDTH), maxWidth);
}

function clampSchemaPanelWidth(width: number, viewportWidth: number): number {
  const maxWidth = Math.max(SCHEMA_PANEL_MIN_WIDTH, viewportWidth - 32);
  return Math.min(Math.max(width, SCHEMA_PANEL_MIN_WIDTH), maxWidth);
}

function getStoredPanelWidth(storageKey: string): number | null {
  if (typeof window === "undefined") {
    return null;
  }

  const rawValue = window.localStorage.getItem(storageKey);
  if (!rawValue) {
    return null;
  }

  const parsedValue = Number(rawValue);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function getDefaultSchemaPanelWidth(viewportWidth: number): number {
  return clampSchemaPanelWidth(Math.round(viewportWidth * 0.95), viewportWidth);
}

function mergeFetchedRunsWithPending(
  currentRuns: RunSummary[],
  fetchedRuns: RunSummary[],
  pendingRunIds: number[],
): RunSummary[] {
  const pendingSet = new Set(pendingRunIds);
  const preservedPendingRuns = currentRuns.filter(
    (run) => pendingSet.has(run.runId) && isRunActive(run) && !fetchedRuns.some((candidate) => candidate.runId === run.runId),
  );

  return [...preservedPendingRuns, ...fetchedRuns.filter((run) => !preservedPendingRuns.some((candidate) => candidate.runId === run.runId))].slice(
    0,
    RECENT_RUN_LIMIT,
  );
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
  const initialViewportWidth = typeof window === "undefined" ? 1440 : window.innerWidth;
  const [view, setView] = useState<ViewState>("connection");
  const [openPanel, setOpenPanel] = useState<WorkbenchPanel>(null);
  const [inspectorMode, setInspectorMode] = useState<InspectorMode>("schema");
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
  const [pendingSparkRunId, setPendingSparkRunId] = useState<number | null>(null);
  const [selectedSparkRunId, setSelectedSparkRunId] = useState<number | null>(null);
  const [sparkComparison, setSparkComparison] = useState<SparkComparisonResponse | null>(null);
  const [error, setError] = useState("");
  const [fallbackNotice, setFallbackNotice] = useState("");
  const [isDesktopPanels, setIsDesktopPanels] = useState<boolean>(isDesktopViewport);
  const [filesPanelWidth, setFilesPanelWidth] = useState<number>(() => {
    const storedWidth = getStoredPanelWidth(FILES_PANEL_STORAGE_KEY);
    return clampFilesPanelWidth(storedWidth ?? FILES_PANEL_DEFAULT_WIDTH, initialViewportWidth);
  });
  const [schemaPanelWidth, setSchemaPanelWidth] = useState<number>(() => {
    const storedWidth = getStoredPanelWidth(SCHEMA_PANEL_STORAGE_KEY);
    return clampSchemaPanelWidth(storedWidth ?? getDefaultSchemaPanelWidth(initialViewportWidth), initialViewportWidth);
  });
  const [resizingPanel, setResizingPanel] = useState<ResizingPanel | null>(null);
  const resizeStateRef = useRef<{ panel: ResizingPanel; startX: number; startWidth: number } | null>(null);
  const recentRunsRef = useRef<RunSummary[]>([]);

  const selectedFile = files.find((item) => item.key === selectedKey) ?? null;
  const displayedSchema = detectedSchema.length > 0 ? detectedSchema : result?.schema ?? [];
  const currentProcessRun = result ? recentRuns.find((run) => run.runId === result.runId) ?? null : null;
  const currentSparkRun = selectedSparkRunId ? recentRuns.find((run) => run.runId === selectedSparkRunId) ?? null : null;
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
  const hasUnsavedOverrides = changedOverrideCount > 0;
  const runIsActive = recentRuns.some(isRunActive);
  const hasPendingRuns = pendingProcessRunId !== null || pendingSparkRunId !== null;
  const latestActiveRun = recentRuns.find(isRunActive) ?? null;
  const activeFileRun = latestActiveRun?.objectKey === selectedKey ? latestActiveRun : null;
  const showingPreviewForSelectedFile = Boolean(result) && (!currentProcessRun || currentProcessRun.objectKey === selectedKey);
  const showProcessWorkspacePrompt = Boolean(selectedFile) && !result && !activeFileRun;
  const canCompareWithSpark = result?.fileType === "csv";
  const selectedFileSummary = selectedFile
    ? `${formatFileFormat(selectedFile)} | ${formatBytes(selectedFile.size)} | Updated ${formatFileTimestamp(selectedFile.lastModified)}`
    : "Open Files & jobs to choose a file and start processing.";
  const activeRunStatusSummary = activeFileRun
    ? `${formatRunType(activeFileRun.runType)} for ${activeFileRun.objectKey} is ${activeFileRun.progressStage || activeFileRun.status}.`
    : "";
  const busyMessage = {
    idle: "",
    listing: "Loading supported files from S3...",
    processing: "Profiling the dataset and generating the processed preview...",
    queueing: "Starting a tracked background job...",
    paging: "Loading the requested preview page...",
    spark: "Queueing the experimental Spark comparison...",
    loadingRun: "Loading the selected run result...",
  }[busyState];

  useEffect(() => {
    recentRunsRef.current = recentRuns;
  }, [recentRuns]);

  useEffect(() => {
    if (typeof window === "undefined") {
      return undefined;
    }

    const syncPanelViewportState = () => {
      const viewportWidth = window.innerWidth;
      const desktop = viewportWidth > DESKTOP_PANEL_BREAKPOINT;
      setIsDesktopPanels(desktop);
      if (!desktop) {
        return;
      }

      setFilesPanelWidth((current) => clampFilesPanelWidth(current, viewportWidth));
      setSchemaPanelWidth((current) => clampSchemaPanelWidth(current, viewportWidth));
    };

    syncPanelViewportState();
    window.addEventListener("resize", syncPanelViewportState);

    return () => {
      window.removeEventListener("resize", syncPanelViewportState);
    };
  }, []);

  useEffect(() => {
    if (!openPanel) {
      document.body.style.overflow = "";
      return undefined;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpenPanel(null);
      }
    };

    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);

    return () => {
      document.body.style.overflow = "";
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [openPanel]);

  useEffect(() => {
    if (!isDesktopPanels || typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(FILES_PANEL_STORAGE_KEY, String(filesPanelWidth));
  }, [filesPanelWidth, isDesktopPanels]);

  useEffect(() => {
    if (!isDesktopPanels || typeof window === "undefined") {
      return;
    }

    window.localStorage.setItem(SCHEMA_PANEL_STORAGE_KEY, String(schemaPanelWidth));
  }, [isDesktopPanels, schemaPanelWidth]);

  useEffect(() => {
    if (!resizingPanel || !isDesktopPanels) {
      return undefined;
    }

    const previousUserSelect = document.body.style.userSelect;
    const previousCursor = document.body.style.cursor;

    const handlePointerMove = (event: PointerEvent) => {
      const resizeState = resizeStateRef.current;
      if (!resizeState) {
        return;
      }

      const viewportWidth = window.innerWidth;
      const deltaX = event.clientX - resizeState.startX;

      if (resizeState.panel === "files") {
        setFilesPanelWidth(clampFilesPanelWidth(resizeState.startWidth + deltaX, viewportWidth));
        return;
      }

      setSchemaPanelWidth(clampSchemaPanelWidth(resizeState.startWidth - deltaX, viewportWidth));
    };

    const handlePointerUp = () => {
      resizeStateRef.current = null;
      setResizingPanel(null);
    };

    document.body.style.userSelect = "none";
    document.body.style.cursor = "ew-resize";
    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", handlePointerUp);

    return () => {
      document.body.style.userSelect = previousUserSelect;
      document.body.style.cursor = previousCursor;
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", handlePointerUp);
    };
  }, [isDesktopPanels, resizingPanel]);

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
          setRecentRuns((current) => mergeFetchedRunsWithPending(current, nextRuns, [pendingProcessRunId, pendingSparkRunId].filter((runId): runId is number => runId !== null)));
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
    if (view !== "workbench" || !selectedKey || (!runIsActive && !hasPendingRuns)) {
      return undefined;
    }

    let isCancelled = false;
    let timeoutId: number | undefined;

    const pollRuns = async () => {
      let nextRuns: RunSummary[] = [];
      let mergedRuns: RunSummary[] = [];
      let shouldContinuePolling = false;

      try {
        nextRuns = await fetchRecentRuns({ objectKey: selectedKey, limit: RECENT_RUN_LIMIT });
        if (isCancelled) {
          return;
        }

        mergedRuns = mergeFetchedRunsWithPending(
          recentRunsRef.current,
          nextRuns,
          [pendingProcessRunId, pendingSparkRunId].filter((runId): runId is number => runId !== null),
        );
        setRecentRuns(mergedRuns);

        if (pendingProcessRunId !== null) {
          const pendingRun = mergedRuns.find((run) => run.runId === pendingProcessRunId);
          if (!pendingRun || isRunActive(pendingRun)) {
            shouldContinuePolling = true;
          } else if (pendingRun.status === "completed") {
            try {
              const nextResult = await loadCompletedProcessRun(pendingRun.runId);
              if (isCancelled) {
                return;
              }
              applyProcessResult(nextResult, nextResult.processingMetadata.appliedOverrides ?? {});
              setPendingProcessRunId(null);
            } catch (caughtError) {
              shouldContinuePolling = true;
              setError(caughtError instanceof Error ? caughtError.message : "Unable to load the completed processing result yet.");
            }
          } else if (pendingRun?.status === "failed") {
            setError(pendingRun.errorMessage || "The background processing job failed.");
            setPendingProcessRunId(null);
          }
        }

        if (pendingSparkRunId !== null) {
          const pendingRun = mergedRuns.find((run) => run.runId === pendingSparkRunId);
          if (!pendingRun || isRunActive(pendingRun)) {
            shouldContinuePolling = true;
          } else if (pendingRun.status === "completed") {
            try {
              await loadSparkComparisonRun(pendingRun.runId, pendingRun.sourceRunId);
              if (isCancelled) {
                return;
              }
              setPendingSparkRunId(null);
            } catch (caughtError) {
              shouldContinuePolling = true;
              setError(caughtError instanceof Error ? caughtError.message : "Unable to load the completed Spark comparison yet.");
            }
          } else if (pendingRun?.status === "failed") {
            setError(pendingRun.errorMessage || "The Spark comparison job failed.");
            setPendingSparkRunId(null);
          }
        }
      } catch (caughtError) {
        if (!isCancelled) {
          setError(caughtError instanceof Error ? caughtError.message : "Unable to refresh the recent job list.");
        }
      } finally {
        const runsToEvaluate = mergedRuns.length > 0 ? mergedRuns : nextRuns;
        if (!isCancelled && (runsToEvaluate.some(isRunActive) || shouldContinuePolling)) {
          const nextInterval = runsToEvaluate.some((run) => run.status === "queued") || shouldContinuePolling ? 1200 : 2500;
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
  }, [hasPendingRuns, pendingProcessRunId, pendingSparkRunId, runIsActive, selectedKey, view]);

  function applyProcessResult(nextResult: ProcessResponse, appliedOverrides: Record<string, string>) {
    setResult(nextResult);
    setCurrentPage(nextResult.previewPage.page);
    setRowsPerPage(nextResult.previewPage.pageSize);
    setDetectedSchema(nextResult.schema);
    setOverrides({
      ...schemaToOverrides(nextResult.schema),
      ...appliedOverrides,
    });
    setInspectorMode("schema");
    setSelectedSparkRunId(null);
    setSparkComparison(null);
  }

  function resetWorkbenchState() {
    setResult(null);
    setDetectedSchema([]);
    setOverrides({});
    setCurrentPage(1);
    setRecentRuns([]);
    setPendingProcessRunId(null);
    setPendingSparkRunId(null);
    setSelectedSparkRunId(null);
    setSparkComparison(null);
    setInspectorMode("schema");
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

  function beginPanelResize(panel: ResizingPanel, clientX: number) {
    if (!isDesktopPanels) {
      return;
    }

    resizeStateRef.current = {
      panel,
      startX: clientX,
      startWidth: panel === "files" ? filesPanelWidth : schemaPanelWidth,
    };
    setResizingPanel(panel);
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
      setView("workbench");
      setOpenPanel("files");
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
      const mergedRuns = mergeFetchedRunsWithPending(
        recentRunsRef.current,
        nextRuns,
        [pendingProcessRunId, pendingSparkRunId].filter((runId): runId is number => runId !== null),
      );
      setRecentRuns(mergedRuns);
      return mergedRuns;
    } catch (caughtError) {
      if (!suppressErrors) {
        setError(caughtError instanceof Error ? caughtError.message : "Unable to refresh the recent job list.");
      }
      return [];
    }
  }

  async function loadCompletedProcessRun(runId: number): Promise<ProcessResponse> {
    const run = await fetchRunStatus(runId);
    const nextResult = buildProcessResponseFromRun(run);
    if (!nextResult) {
      throw new Error("The selected run is not a completed Pandas processing result.");
    }
    return nextResult;
  }

  async function loadSparkComparisonRun(runId: number, sourceRunId?: number | null): Promise<SparkComparisonResponse> {
    const comparisonRun = await fetchRunStatus(runId);
    if (!comparisonRun.sparkComparison) {
      throw new Error("The selected run does not contain a completed Spark comparison.");
    }

    const comparisonSourceRunId = comparisonRun.sourceRunId ?? sourceRunId ?? null;
    if (comparisonSourceRunId && result?.runId !== comparisonSourceRunId) {
      const sourceResult = await loadCompletedProcessRun(comparisonSourceRunId);
      applyProcessResult(sourceResult, sourceResult.processingMetadata.appliedOverrides ?? {});
    }

    setInspectorMode("spark");
    setSelectedSparkRunId(comparisonRun.runId);
    setSparkComparison(comparisonRun.sparkComparison);
    return comparisonRun.sparkComparison;
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
    setOpenPanel(null);
    setSelectedSparkRunId(null);
    setSparkComparison(null);
    setInspectorMode("schema");

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
    } catch (caughtError) {
      if (caughtError instanceof ApiError && (caughtError.code === "task_queue_error" || caughtError.status === 503)) {
        setFallbackNotice("Background queueing was unavailable, so the app processed this file inline instead.");
        await runSynchronousProcess(appliedOverrideMap);
        return;
      }

      setError(caughtError instanceof Error ? caughtError.message : "Unable to queue background processing.");
    } finally {
      setBusyState("idle");
    }
  }

  async function handleRunSparkComparison() {
    if (!hasConnectionDetails) {
      setError(`Enter ${missingConnectionFields.join(", ")} before running the Spark comparison.`);
      return;
    }
    if (!result || result.fileType !== "csv") {
      setError("Complete a CSV Pandas run before comparing it with Spark.");
      return;
    }

    setBusyState("spark");
    setError("");
    setSelectedSparkRunId(null);
    setSparkComparison(null);
    setInspectorMode("spark");

    try {
      const queuedRun = await runSparkComparison({
        credentials,
        sourceRunId: result.runId,
        page: activePage,
        pageSize: rowsPerPage,
      });
      setPendingSparkRunId(queuedRun.runId);
      upsertQueuedRun(
        buildQueuedRunSummary({
          ...queuedRun,
          bucket: credentials.bucket,
          objectKey: selectedKey,
          fileType: result.fileType,
          selectedSheet: result.selectedSheet,
        }),
      );
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to queue the Spark comparison.");
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
      const nextResult = await loadCompletedProcessRun(runId);
      applyProcessResult(nextResult, nextResult.processingMetadata.appliedOverrides ?? {});
      setOpenPanel(null);
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to load the selected run result.");
    } finally {
      setBusyState("idle");
    }
  }

  async function handleViewSparkComparisonRun(runId: number, sourceRunId?: number | null) {
    setBusyState("loadingRun");
    setError("");

    try {
      await loadSparkComparisonRun(runId, sourceRunId);
      setOpenPanel("schema");
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : "Unable to load the selected Spark comparison.");
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
    setOpenPanel(null);
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
                {activeFileRun ? <span className="summary-chip-live">Processing in background</span> : null}
              </div>
              <div className="workbench-header-buttons">
                <button className="secondary-button panel-toggle-button" onClick={() => setOpenPanel("files")}>
                  Files & jobs
                </button>
                <button className="secondary-button panel-toggle-button" onClick={() => setOpenPanel("schema")}>
                  Schema & tools
                </button>
                <button className="secondary-button" onClick={handleEditConnection}>
                  Edit connection
                </button>
              </div>
            </div>
          </section>

          {error ? <div className="callout danger">{error}</div> : null}

          <section className="workbench-shell">
            {openPanel ? (
              <button className="workbench-panel-backdrop" aria-label="Close workbench side panel" onClick={() => setOpenPanel(null)} />
            ) : null}

            <aside
              className={`card workbench-panel workbench-panel-left ${openPanel === "files" ? "workbench-panel-open" : ""}`}
              aria-label="Files and jobs"
              aria-hidden={openPanel !== "files"}
              style={isDesktopPanels ? { width: `${filesPanelWidth}px` } : undefined}
            >
              {isDesktopPanels ? (
                <div
                  className={`panel-resize-handle panel-resize-handle-left ${resizingPanel === "files" ? "is-resizing" : ""}`}
                  role="separator"
                  aria-label="Resize Files and jobs panel"
                  aria-orientation="vertical"
                  onPointerDown={(event) => {
                    event.preventDefault();
                    beginPanelResize("files", event.clientX);
                  }}
                />
              ) : null}
              <div className="panel-header">
                <div>
                  <p className="section-label">File selection + jobs</p>
                  <h2>Choose a file</h2>
                </div>
                <button className="secondary-button panel-close-button" onClick={() => setOpenPanel(null)}>
                  Close panel
                </button>
              </div>

              <section className="rail-section">
                <div className="rail-section-header">
                  <div>
                    <p className="section-label">Available files</p>
                    <h3>Supported objects</h3>
                  </div>
                  <span className="section-stat">{files.length}</span>
                </div>
                {files.length === 0 ? (
                  <div className="empty-state compact">
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
                          {file.format.toUpperCase()} | {formatBytes(file.size)}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </section>

              <section className="rail-section selected-file-panel">
                <div className="rail-section-header">
                  <div>
                    <p className="section-label">Selected file</p>
                    <h3>{selectedFile?.key ?? "No file selected"}</h3>
                  </div>
                </div>
                <p className="helper-text">{selectedFileSummary}</p>

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
              </section>

              <section className="rail-section process-panel">
                <div className="rail-section-header">
                  <div>
                    <p className="section-label">Primary action</p>
                    <h3>Process this file</h3>
                  </div>
                </div>
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
                <p className="helper-text">Infer schema and build the first preview page for the selected dataset.</p>
                {fallbackNotice ? <p className="inline-notice">{fallbackNotice}</p> : null}
                {busyState === "listing" || busyState === "queueing" || busyState === "processing" ? (
                  <LoadingNotice message={busyMessage} />
                ) : null}
              </section>

              {activeFileRun ? (
                <section className="rail-section active-job-panel">
                  <div className="rail-section-header">
                    <div>
                      <p className="section-label">Active job</p>
                      <h3>{activeFileRun.objectKey}</h3>
                    </div>
                    <span className={`job-status job-status-${activeFileRun.status}`}>{activeFileRun.status}</span>
                  </div>
                  <p className="job-meta">{formatJobMeta(activeFileRun)}</p>
                  <div className="job-progress-row">
                    <span>{activeFileRun.progressStage || activeFileRun.status}</span>
                    <strong>{activeFileRun.progressPercent}%</strong>
                  </div>
                  <LoadingNotice message={activeRunStatusSummary} />
                </section>
              ) : null}

              <section className="rail-section jobs-section">
                <div className="rail-section-header">
                  <div>
                    <p className="section-label">Tracked jobs</p>
                    <h3>Recent runs</h3>
                  </div>
                </div>
                {recentRuns.length > 0 ? (
                  <div className="job-list compact">
                    {recentRuns.map((run) => {
                      const isCurrentResult = result?.runId === run.runId;
                      const isCurrentComparison = selectedSparkRunId === run.runId;
                      const canRetry = run.runType === "process" && run.status === "failed" && run.objectKey === selectedKey;
                      const canViewResult = run.runType === "process" && run.status === "completed";
                      const canViewComparison = run.runType === "spark_compare" && run.status === "completed";
                      const shouldShowProgress = isRunActive(run) || run.status === "failed";

                      return (
                        <article
                          key={run.runId}
                          className={`job-card ${isCurrentResult || isCurrentComparison ? "job-card-current" : ""} ${
                            isRunActive(run) ? "job-card-live" : ""
                          }`}
                        >
                          <div className="job-card-header">
                            <div className="job-card-copy">
                              <p className="job-title">{run.objectKey}</p>
                              <p className="job-meta">{formatJobMeta(run)}</p>
                            </div>
                            <span className={`job-status job-status-${run.status}`}>{run.status}</span>
                          </div>
                          {shouldShowProgress ? (
                            <div className="job-progress-row">
                              <span>{run.progressStage || run.status}</span>
                              <strong>{run.progressPercent}%</strong>
                            </div>
                          ) : null}
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
                            {canViewComparison ? (
                              <button
                                className="secondary-button"
                                onClick={() => {
                                  void handleViewSparkComparisonRun(run.runId, run.sourceRunId);
                                }}
                                disabled={busyState !== "idle" || isCurrentComparison}
                              >
                                {isCurrentComparison ? "Viewing comparison" : "View comparison"}
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
                ) : (
                  <div className="empty-state compact">
                    <p>Process the selected file to build the first preview and start a visible run history.</p>
                  </div>
                )}
              </section>
            </aside>

            <section className="workbench-center">
              <article className="card preview-card" aria-label="Results workspace">
                <div className="card-header compact workspace-header">
                  <div>
                    <p className="section-label">Results workspace</p>
                    <h2>Processed preview</h2>
                    <p className="helper-text">
                      {selectedFile
                        ? `Working with ${selectedFile.key}. Results and pagination stay here once processing completes.`
                        : "Open Files & jobs to choose a supported dataset and start building a preview."}
                    </p>
                  </div>
                  <div className="workspace-header-actions">
                    {activeFileRun ? <span className="status-chip status-chip-live">Processing in background</span> : null}
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
                </div>

                {busyState === "paging" || busyState === "loadingRun" ? <LoadingNotice message={busyMessage} /> : null}

                {activeFileRun ? (
                  <section className="workspace-status">
                    <div className="workspace-status-header">
                      <strong>Processing {activeFileRun.objectKey}</strong>
                      <span>
                        {activeFileRun.progressStage || activeFileRun.status} | {activeFileRun.progressPercent}%
                      </span>
                    </div>
                    <p>
                      {showingPreviewForSelectedFile
                        ? "Your last completed preview stays visible while the new run finishes."
                        : "The first preview and schema will appear here automatically when the run completes."}
                    </p>
                  </section>
                ) : null}

                {!selectedFile ? (
                  <div className="empty-state workspace-empty-state">
                    <h3>Choose a file to begin</h3>
                    <p>
                      Open Files & jobs to choose a supported CSV or Excel file. The schema inspector and preview will
                      update automatically after processing.
                    </p>
                    <button className="secondary-button workspace-open-button" onClick={() => setOpenPanel("files")}>
                      Open Files & jobs
                    </button>
                  </div>
                ) : showProcessWorkspacePrompt ? (
                  <div className="empty-state workspace-empty-state ready-state">
                    <p className="section-label">Ready to process</p>
                    <h3>{selectedFile.key}</h3>
                    <p>{selectedFileSummary}</p>
                    <button className="secondary-button workspace-open-button" onClick={() => setOpenPanel("files")}>
                      Open Files & jobs
                    </button>
                    <p className="helper-text">
                      Use the control panel to run the selected file, build the first preview page, and unlock override
                      controls.
                    </p>
                  </div>
                ) : result ? (
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
                  <div className="empty-state workspace-empty-state">
                    <h3>Waiting for results</h3>
                    <p>Open Files & jobs to process the selected file and reveal the schema tools in the inspector.</p>
                  </div>
                )}
              </article>
            </section>

            <aside
              className={`card workbench-panel workbench-panel-right ${openPanel === "schema" ? "workbench-panel-open" : ""}`}
              aria-label="Schema and comparison tools"
              aria-hidden={openPanel !== "schema"}
              style={isDesktopPanels ? { width: `${schemaPanelWidth}px` } : undefined}
            >
              {isDesktopPanels ? (
                <div
                  className={`panel-resize-handle panel-resize-handle-right ${resizingPanel === "schema" ? "is-resizing" : ""}`}
                  role="separator"
                  aria-label="Resize Schema and comparison tools panel"
                  aria-orientation="vertical"
                  onPointerDown={(event) => {
                    event.preventDefault();
                    beginPanelResize("schema", event.clientX);
                  }}
                />
              ) : null}
                <div className="panel-header">
                  <div className="panel-header-title">
                    <p className="section-label">Schema + comparison</p>
                    <h2>Review and refine</h2>
                  </div>
                  <div className="panel-header-actions">
                    <button className="secondary-button panel-close-button" onClick={() => setOpenPanel(null)}>
                      Close panel
                    </button>
                  </div>
                </div>

              {!selectedFile ? (
                <div className="empty-state inspector-empty-state">
                  <h3>Select a file first</h3>
                  <p>Open Files & jobs, choose a file, and then process it to unlock schema review and optional Spark comparison tools.</p>
                </div>
              ) : !result ? (
                <div className="empty-state inspector-empty-state">
                  <h3>Schema tools appear after processing</h3>
                  <p>Run processing to infer schema, review warnings, and enable override controls for {selectedFile.key}.</p>
                </div>
              ) : (
                <>
                  <div className="inspector-header">
                    <div className="inspector-tabs" role="tablist" aria-label="Inspector modes">
                      <button
                        className={`inspector-tab ${inspectorMode === "schema" ? "active" : ""}`}
                        onClick={() => setInspectorMode("schema")}
                        type="button"
                      >
                        Schema
                      </button>
                      {sparkComparison ? (
                        <button
                          className={`inspector-tab ${inspectorMode === "spark" ? "active" : ""}`}
                          onClick={() => setInspectorMode("spark")}
                          type="button"
                        >
                          Spark comparison
                        </button>
                        ) : null}
                      </div>
                      <div className="inspector-header-actions">
                        {result && inspectorMode === "schema" && canCompareWithSpark ? (
                          <section className="advanced-section advanced-section-inline">
                            <div>
                              <p className="section-label">Experimental</p>
                              <h3>Compare engines</h3>
                            </div>
                            <div className="advanced-actions">
                              <button
                                className="secondary-button"
                                onClick={handleRunSparkComparison}
                                disabled={busyState !== "idle" || runIsActive}
                              >
                                {pendingSparkRunId !== null ? "Queueing comparison..." : "Compare with Spark (experimental)"}
                              </button>
                            </div>
                            <p className="helper-text">Run Spark on this completed CSV without replacing the current Pandas result.</p>
                          </section>
                        ) : null}
                        {hasUnsavedOverrides ? <span className="status-chip status-chip-warning">Overrides changed</span> : null}
                      </div>
                    </div>

                    {inspectorMode === "spark" && sparkComparison ? (
                    <section className="comparison-panel">
                      <div className="card-header compact">
                        <div>
                          <p className="section-label">Experimental</p>
                          <h2>Compare with Spark</h2>
                          <p className="helper-text">
                            This comparison uses the same CSV source as the selected Pandas run. Pandas remains the
                            authoritative inference pipeline for the app.
                          </p>
                        </div>
                      </div>
                      <div className="metrics-row">
                        <div className="metric">
                          <span className="metric-label">Pandas runtime</span>
                          <strong>{result.processingMetadata.durationMs} ms</strong>
                        </div>
                        <div className="metric">
                          <span className="metric-label">Spark runtime</span>
                          <strong>{sparkComparison.processingMetadata.durationMs} ms</strong>
                        </div>
                        <div className="metric">
                          <span className="metric-label">Pandas rows</span>
                          <strong>{result.rowCount.toLocaleString()}</strong>
                        </div>
                        <div className="metric">
                          <span className="metric-label">Spark rows</span>
                          <strong>{sparkComparison.rowCount.toLocaleString()}</strong>
                        </div>
                      </div>
                      {currentSparkRun ? <p className="helper-text">Viewing {formatJobMeta(currentSparkRun)}.</p> : null}
                      <div className="callout warning">
                        <h3>Comparison note</h3>
                        <ul>
                          {sparkComparison.notes.map((note) => (
                            <li key={note}>{note}</li>
                          ))}
                        </ul>
                      </div>
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
                        <div className="comparison-preview-header">
                          <div>
                            <h3>Spark preview</h3>
                            <p>
                              Rows {(sparkComparison.previewPage.page - 1) * sparkComparison.previewPage.pageSize + 1}-
                              {(sparkComparison.previewPage.page - 1) * sparkComparison.previewPage.pageSize +
                                sparkComparison.previewRows.length}{" "}
                              of {sparkComparison.previewPage.totalRows}
                            </p>
                          </div>
                          <span className="comparison-badge">{sparkComparison.processingMetadata.sparkMaster}</span>
                        </div>
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
                    </section>
                  ) : (
                    <div className="schema-workspace">
                      <div className="schema-topbar">
                        <section className="schema-overview">
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
                        </section>

                        <div className="inspector-toolbar">
                          <button
                            className="secondary-button"
                            onClick={applySchemaDefaults}
                            disabled={busyState === "processing" || busyState === "queueing" || displayedSchema.length === 0}
                          >
                            Reset overrides
                          </button>
                          <button className="secondary-button" onClick={handleProcessFile} disabled={busyState !== "idle" || runIsActive}>
                            Reprocess with overrides
                          </button>
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
                    </div>
                  )}
                </>
              )}
            </aside>
          </section>
        </>
      )}
    </main>
  );
}
