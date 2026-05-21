import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./App";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function enterRequiredCredentials() {
  fireEvent.change(screen.getByLabelText(/Access key ID/i), { target: { value: "AKIA123" } });
  fireEvent.change(screen.getByLabelText(/Secret access key/i), { target: { value: "secret" } });
  fireEvent.change(screen.getByLabelText(/^Bucket$/i), { target: { value: "demo-bucket" } });
}

function jsonResponse(body: unknown, init?: Partial<Response>): Partial<Response> {
  return {
    ok: init?.ok ?? true,
    status: init?.status ?? 200,
    text: async () => JSON.stringify(body),
  };
}

function buildRecentRun(overrides?: Partial<Record<string, unknown>>) {
  return {
    runId: 1,
    taskId: "task-123",
    runType: "process",
    sourceRunId: null,
    status: "completed",
    engine: "pandas",
    bucket: "demo-bucket",
    objectKey: "sample.csv",
    progressStage: "completed",
    progressPercent: 100,
    errorMessage: "",
    createdAt: "2026-05-21T00:00:00Z",
    startedAt: "2026-05-21T00:00:01Z",
    completedAt: "2026-05-21T00:00:02Z",
    fileType: "csv",
    selectedSheet: "",
    ...overrides,
  };
}

function buildProcessResponse(
  overrides?: Partial<{
    runId: number;
    rowCount: number;
    schema: Array<Record<string, unknown>>;
    previewColumns: string[];
    previewRows: Array<Record<string, unknown>>;
    previewPage: Record<string, unknown>;
    processingMetadata: Record<string, unknown>;
  }>,
) {
  const previewRows = overrides?.previewRows ?? [{ Score: 90 }, { Score: 75 }];
  const rowCount = overrides?.rowCount ?? previewRows.length;
  return {
    runId: overrides?.runId ?? 1,
    rowCount,
    schema: overrides?.schema ?? [
      {
        column: "Score",
        inferred_type: "float",
        storage_type: "Float64",
        display_type: "Float",
        nullable: true,
        confidence: 0.97,
        warnings: [],
        null_token_count: 1,
        sample_values: ["90", "75"],
        allowed_overrides: ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
      },
    ],
    previewColumns: overrides?.previewColumns ?? ["Score"],
    previewRows,
    previewPage: overrides?.previewPage ?? {
      page: 1,
      pageSize: 25,
      totalRows: rowCount,
      totalPages: Math.max(1, Math.ceil(rowCount / 25)),
      hasPreviousPage: false,
      hasNextPage: rowCount > 25,
    },
    warnings: [],
    processingMetadata: {
      durationMs: 11.2,
      previewRowLimit: 100,
      chunkSize: 5000,
      appliedOverrides: {},
      ...overrides?.processingMetadata,
    },
    selectedSheet: "",
    fileType: "csv",
  };
}

describe("App", () => {
  it("switches to the workbench after browsing files and preserves credentials when editing the connection", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse({
            files: [{ key: "folder/sample.csv", size: 128, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
          }),
        )
        .mockResolvedValueOnce(jsonResponse({ runs: [] })),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: /folder\/sample\.csv/i })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Edit connection/i }));

    expect(screen.getByText("S3 connection")).toBeInTheDocument();
    expect(screen.getByDisplayValue("AKIA123")).toBeInTheDocument();
    expect(screen.getByDisplayValue("secret")).toBeInTheDocument();
    expect(screen.getByDisplayValue("demo-bucket")).toBeInTheDocument();
  });

  it("renders schema results and preview rows after falling back to synchronous processing", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse({
            files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
          }),
        )
        .mockResolvedValueOnce(jsonResponse({ runs: [] }))
        .mockResolvedValueOnce(jsonResponse({ detail: "Queue unavailable", code: "task_queue_error" }, { ok: false, status: 503 }))
        .mockResolvedValueOnce(jsonResponse(buildProcessResponse()))
        .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun()] })),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process file/i }));

    await waitFor(() => expect(screen.getByText("Inferred schema")).toBeInTheDocument());
    expect(screen.getByRole("cell", { name: "Score" })).toBeInTheDocument();
    expect(screen.getByLabelText(/Override type for Score/i)).toHaveValue("float");
    expect(screen.getByText("Processed preview")).toBeInTheDocument();
    expect(screen.getByText(/Preview rows 1-2 of 2/i)).toBeInTheDocument();
    expect(screen.getByText(/Background queueing was unavailable/i)).toBeInTheDocument();
  });

  it("resets overrides back to the detected schema after reprocessing", async () => {
    const fetchMock = vi.fn();
    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ runs: [] }))
      .mockResolvedValueOnce(jsonResponse({ detail: "Queue unavailable", code: "task_queue_error" }, { ok: false, status: 503 }))
      .mockResolvedValueOnce(jsonResponse(buildProcessResponse()))
      .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun()] }))
      .mockResolvedValueOnce(jsonResponse({ detail: "Queue unavailable", code: "task_queue_error" }, { ok: false, status: 503 }))
      .mockResolvedValueOnce(
        jsonResponse(
          buildProcessResponse({
            runId: 2,
            previewRows: [{ Score: "90" }, { Score: "75" }],
            processingMetadata: { appliedOverrides: { Score: "text" } },
          }),
        ),
      )
      .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun({ runId: 2 }), buildRecentRun()] }));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process file/i }));
    await waitFor(() => expect(screen.getByLabelText(/Override type for Score/i)).toHaveValue("float"));

    fireEvent.change(screen.getByLabelText(/Override type for Score/i), { target: { value: "text" } });
    expect(screen.getByLabelText(/Override type for Score/i)).toHaveValue("text");

    fireEvent.click(screen.getByRole("button", { name: /Reprocess with overrides/i }));
    await waitFor(() => expect(screen.getByLabelText(/Override type for Score/i)).toHaveValue("text"));
    expect(screen.getByRole("cell", { name: "Float" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Reset overrides/i }));
    expect(screen.getByLabelText(/Override type for Score/i)).toHaveValue("float");
  });

  it("paginates preview rows on the client and resets paging on reprocess and page size change", async () => {
    const firstPageRows = Array.from({ length: 25 }, (_, index) => ({ Index: index + 1 }));
    const secondPageRows = Array.from({ length: 5 }, (_, index) => ({ Index: index + 26 }));
    const firstPageResponse = buildProcessResponse({
      rowCount: 30,
      schema: [
        {
          column: "Index",
          inferred_type: "integer",
          storage_type: "Int64",
          display_type: "Integer",
          nullable: false,
          confidence: 0.98,
          warnings: [],
          null_token_count: 0,
          sample_values: ["1", "2", "3"],
          allowed_overrides: ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
        },
      ],
      previewColumns: ["Index"],
      previewRows: firstPageRows,
      previewPage: {
        page: 1,
        pageSize: 25,
        totalRows: 30,
        totalPages: 2,
        hasPreviousPage: false,
        hasNextPage: true,
      },
    });
    const secondPageResponse = {
      runId: 1,
      rowCount: 30,
      previewColumns: ["Index"],
      previewRows: secondPageRows,
      previewPage: {
        page: 2,
        pageSize: 25,
        totalRows: 30,
        totalPages: 2,
        hasPreviousPage: true,
        hasNextPage: false,
      },
    };
    const resizedFirstPageResponse = {
      runId: 2,
      rowCount: 30,
      previewColumns: ["Index"],
      previewRows: Array.from({ length: 30 }, (_, index) => ({ Index: index + 1 })),
      previewPage: {
        page: 1,
        pageSize: 50,
        totalRows: 30,
        totalPages: 1,
        hasPreviousPage: false,
        hasNextPage: false,
      },
    };

    const fetchMock = vi.fn();
    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ runs: [] }))
      .mockResolvedValueOnce(jsonResponse({ detail: "Queue unavailable", code: "task_queue_error" }, { ok: false, status: 503 }))
      .mockResolvedValueOnce(jsonResponse(firstPageResponse))
      .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun()] }))
      .mockResolvedValueOnce(jsonResponse(secondPageResponse))
      .mockResolvedValueOnce(jsonResponse(firstPageResponse))
      .mockResolvedValueOnce(jsonResponse(secondPageResponse))
      .mockResolvedValueOnce(jsonResponse({ detail: "Queue unavailable", code: "task_queue_error" }, { ok: false, status: 503 }))
      .mockResolvedValueOnce(jsonResponse({ ...firstPageResponse, runId: 2 }))
      .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun({ runId: 2 }), buildRecentRun()] }))
      .mockResolvedValueOnce(jsonResponse({ ...secondPageResponse, runId: 2 }))
      .mockResolvedValueOnce(jsonResponse(resizedFirstPageResponse));

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process file/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 1-25 of 30/i)).toBeInTheDocument());
    expect(screen.getByRole("cell", { name: "1" })).toBeInTheDocument();
    expect(screen.queryByRole("cell", { name: "26" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Next page/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 26-30 of 30/i)).toBeInTheDocument());
    expect(screen.getByRole("cell", { name: "26" })).toBeInTheDocument();
    expect(JSON.parse(String(fetchMock.mock.calls[5]?.[1]?.body))).toMatchObject({
      run_id: 1,
      object_key: "sample.csv",
      file_type: "csv",
      row_count: 30,
      preview_columns: ["Index"],
    });

    fireEvent.click(screen.getByRole("button", { name: /Previous page/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 1-25 of 30/i)).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Next page/i }));
    await waitFor(() => expect(screen.getByText(/Page 2 of 2/i)).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Reprocess with overrides/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 1-25 of 30/i)).toBeInTheDocument());
    expect(screen.getByText(/Page 1 of 2/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Next page/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 26-30 of 30/i)).toBeInTheDocument());

    fireEvent.change(screen.getByLabelText(/Rows per page/i), { target: { value: "50" } });
    await waitFor(() => expect(screen.getByText(/Preview rows 1-30 of 30/i)).toBeInTheDocument());
    expect(screen.getByText(/Page 1 of 1/i)).toBeInTheDocument();
  });

  it("shows a stable empty state when browsing returns no supported files", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        jsonResponse({
          files: [],
        }),
      ),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());
    expect(screen.getByText(/No supported files were found for this bucket or prefix/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Process file/i })).toBeDisabled();
  });

  it("allows the files and schema drawer to be collapsed without losing the preview", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse({
            files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
          }),
        )
        .mockResolvedValueOnce(jsonResponse({ runs: [] }))
        .mockResolvedValueOnce(jsonResponse({ detail: "Queue unavailable", code: "task_queue_error" }, { ok: false, status: 503 }))
        .mockResolvedValueOnce(jsonResponse(buildProcessResponse()))
        .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun()] })),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process file/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 1-2 of 2/i)).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Hide files & schema/i }));
    expect(screen.queryByText("Supported files")).not.toBeInTheDocument();
    expect(screen.getByText("Processed preview")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Open files & schema/i }));
    expect(screen.getByText("Supported files")).toBeInTheDocument();
  });

  it("prefers async processing, shows recent jobs, and hydrates the preview on completion", async () => {
    const processResponse = buildProcessResponse({ runId: 7 });
    const completedRun = {
      ...processResponse,
      taskId: "task-123",
      runType: "process",
      sourceRunId: null,
      status: "completed",
      engine: "pandas",
      bucket: "demo-bucket",
      objectKey: "sample.csv",
      progressStage: "completed",
      progressPercent: 100,
      errorMessage: "",
      createdAt: "2026-05-21T00:00:00Z",
      startedAt: "2026-05-21T00:00:01Z",
      completedAt: "2026-05-21T00:00:02Z",
    };

    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce(
          jsonResponse({
            files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
          }),
        )
        .mockResolvedValueOnce(jsonResponse({ runs: [] }))
        .mockResolvedValueOnce(jsonResponse({ runId: 7, taskId: "task-123", runType: "process", status: "queued", engine: "pandas" }))
        .mockResolvedValueOnce(
          jsonResponse({
            runs: [
              buildRecentRun({
                runId: 7,
                taskId: "task-123",
                status: "queued",
                progressStage: "queued",
                progressPercent: 0,
              }),
            ],
          }),
        )
        .mockResolvedValueOnce(jsonResponse({ runs: [buildRecentRun({ runId: 7 })] }))
        .mockResolvedValueOnce(jsonResponse(completedRun)),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process file/i }));
    await waitFor(() => expect(screen.getByText("Tracked processing")).toBeInTheDocument());
    expect(screen.getAllByText("sample.csv").length).toBeGreaterThan(0);

    await waitFor(() => expect(screen.getByText(/Preview rows 1-2 of 2/i)).toBeInTheDocument());
    expect(screen.getByRole("cell", { name: "Score" })).toBeInTheDocument();
  });

  it("runs the experimental Spark comparison for CSV files and disables it for Excel", async () => {
    const fetchMock = vi.fn();
    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
        }),
      )
      .mockResolvedValueOnce(jsonResponse({ runs: [] }))
      .mockResolvedValueOnce(
        jsonResponse({
          engine: "spark",
          fileType: "csv",
          objectKey: "sample.csv",
          rowCount: 2,
          sparkSchema: [
            { column: "Score", sparkType: "int", mappedType: "integer", displayType: "Integer", nullable: false },
          ],
          previewColumns: ["Score"],
          previewRows: [{ Score: "90" }, { Score: "75" }],
          previewPage: {
            page: 1,
            pageSize: 25,
            totalRows: 2,
            totalPages: 1,
            hasPreviousPage: false,
            hasNextPage: false,
          },
          processingMetadata: { durationMs: 32.1, pageSize: 25, sparkMaster: "local[*]" },
          notes: ["Experimental comparison mode."],
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          files: [{ key: "workbook.xlsx", size: 512, lastModified: "2026-04-03T00:00:00Z", format: "excel" }],
        }),
      );

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Run Spark comparison/i }));
    await waitFor(() => expect(screen.getByText("Spark comparison")).toBeInTheDocument());
    expect(screen.getByText("Experimental comparison mode.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Edit connection/i }));
    fireEvent.change(screen.getByLabelText(/^Bucket$/i), { target: { value: "demo-bucket" } });
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    expect(screen.getByRole("button", { name: /Run Spark comparison/i })).toBeDisabled();
  });
});
