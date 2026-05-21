export type S3CredentialsInput = {
  access_key_id: string;
  secret_access_key: string;
  session_token: string;
  region: string;
  bucket: string;
  prefix: string;
};

export type S3File = {
  key: string;
  size: number;
  lastModified: string | null;
  format: "csv" | "excel";
};

export type ColumnInferenceResult = {
  column: string;
  inferred_type: string;
  storage_type: string;
  display_type: string;
  nullable: boolean;
  confidence: number;
  warnings: string[];
  null_token_count: number;
  sample_values: string[];
  allowed_overrides: string[];
};

export type ProcessResponse = {
  runId: number;
  rowCount: number;
  schema: ColumnInferenceResult[];
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
  previewPage: {
    page: number;
    pageSize: number;
    totalRows: number;
    totalPages: number;
    hasPreviousPage: boolean;
    hasNextPage: boolean;
  };
  warnings: string[];
  processingMetadata: {
    durationMs: number;
    previewRowLimit: number;
    chunkSize: number | null;
    appliedOverrides?: Record<string, string>;
  };
  selectedSheet: string;
  fileType: "csv" | "excel";
};

export type ProcessAsyncResponse = {
  runId: number;
  taskId: string;
  runType: "process" | "spark_compare";
  sourceRunId?: number | null;
  status: "queued" | "processing" | "completed" | "failed";
  engine: "pandas" | "spark";
};

export type RunSummary = {
  runId: number;
  taskId: string;
  runType: "process" | "spark_compare";
  sourceRunId?: number | null;
  status: "queued" | "processing" | "completed" | "failed";
  engine: "pandas" | "spark";
  bucket: string;
  objectKey: string;
  progressStage: string;
  progressPercent: number;
  errorMessage: string;
  createdAt: string;
  startedAt?: string | null;
  completedAt?: string | null;
  fileType?: "csv" | "excel";
  selectedSheet?: string;
};

export type PreviewPageResponse = {
  runId: number;
  rowCount: number;
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
  previewPage: {
    page: number;
    pageSize: number;
    totalRows: number;
    totalPages: number;
    hasPreviousPage: boolean;
    hasNextPage: boolean;
  };
};

export type RunStatusResponse = RunSummary & {
  rowCount?: number;
  schema?: ColumnInferenceResult[];
  previewColumns?: string[];
  previewRows?: Record<string, unknown>[];
  previewPage?: {
    page: number;
    pageSize: number;
    totalRows: number;
    totalPages: number;
    hasPreviousPage: boolean;
    hasNextPage: boolean;
  };
  warnings?: string[];
  processingMetadata?: {
    durationMs: number;
    previewRowLimit?: number;
    chunkSize?: number | null;
    appliedOverrides?: Record<string, string>;
  };
  selectedSheet?: string;
  fileType?: "csv" | "excel";
  sparkComparison?: SparkComparisonResponse;
};

export type SparkComparisonResponse = {
  engine: "spark";
  fileType: "csv";
  objectKey: string;
  rowCount: number;
  sparkSchema: Array<{
    column: string;
    sparkType: string;
    mappedType: string;
    displayType: string;
    nullable: boolean;
  }>;
  previewColumns: string[];
  previewRows: Record<string, unknown>[];
  previewPage: {
    page: number;
    pageSize: number;
    totalRows: number;
    totalPages: number;
    hasPreviousPage: boolean;
    hasNextPage: boolean;
  };
  processingMetadata: {
    durationMs: number;
    pageSize: number;
    sparkMaster: string;
  };
  notes: string[];
};
