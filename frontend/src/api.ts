import type { PreviewPageResponse, ProcessResponse, S3CredentialsInput, S3File } from "./types";

async function apiFetch<T>(path: string, payload: Record<string, unknown>): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });
  } catch {
    throw new Error("The server could not be reached while processing this file. Please try again.");
  }

  const rawBody =
    typeof response.text === "function"
      ? await response.text()
      : JSON.stringify(await response.json().catch(() => ({})));
  const body = rawBody
    ? (() => {
        try {
          return JSON.parse(rawBody) as Record<string, unknown>;
        } catch {
          return {};
        }
      })()
    : {};
  if (!response.ok) {
    if (typeof body.detail === "string") {
      throw new Error(body.detail);
    }
    if (response.status >= 500) {
      throw new Error(
        "The server ran out of processing resources for this file. Try a smaller preview page or a smaller file.",
      );
    }
    throw new Error(rawBody || "Request failed.");
  }

  return body as T;
}

export async function fetchS3Files(credentials: S3CredentialsInput): Promise<S3File[]> {
  const response = await apiFetch<{ files: S3File[] }>("/api/s3/files", credentials);
  return response.files;
}

export async function processFile(payload: {
  credentials: S3CredentialsInput;
  objectKey: string;
  sheetName?: string;
  overrides?: Array<{ column: string; target_type: string }>;
  previewRowLimit?: number;
}): Promise<ProcessResponse> {
  return apiFetch<ProcessResponse>("/api/data/process", {
    ...payload.credentials,
    object_key: payload.objectKey,
    sheet_name: payload.sheetName ?? "",
    overrides: payload.overrides ?? [],
    preview_row_limit: payload.previewRowLimit ?? 100,
  });
}

export async function fetchPreviewPage(payload: {
  credentials: S3CredentialsInput;
  runId: number;
  page: number;
  pageSize: number;
}): Promise<PreviewPageResponse> {
  return apiFetch<PreviewPageResponse>("/api/data/preview", {
    ...payload.credentials,
    run_id: payload.runId,
    page: payload.page,
    page_size: payload.pageSize,
  });
}
