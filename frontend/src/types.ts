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
  warnings: string[];
  processingMetadata: {
    durationMs: number;
    previewRowLimit: number;
    chunkSize: number | null;
  };
  selectedSheet: string;
  fileType: "csv" | "excel";
};

