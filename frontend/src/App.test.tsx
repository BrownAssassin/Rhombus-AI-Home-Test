import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./App";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function enterRequiredCredentials() {
  fireEvent.change(screen.getByLabelText(/Access key ID/i), { target: { value: "AKIA123" } });
  fireEvent.change(screen.getByLabelText(/Secret access key/i), { target: { value: "secret" } });
  fireEvent.change(screen.getByLabelText(/^Bucket$/i), { target: { value: "demo-bucket" } });
}

function buildProcessResponse(overrides?: Partial<{
  runId: number;
  schema: Array<Record<string, unknown>>;
  previewColumns: string[];
  previewRows: Array<Record<string, unknown>>;
}>) {
  return {
    runId: overrides?.runId ?? 1,
    rowCount: 5,
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
    previewRows: overrides?.previewRows ?? [{ Score: 90 }, { Score: 75 }],
    warnings: [],
    processingMetadata: { durationMs: 11.2, previewRowLimit: 100, chunkSize: 5000 },
    selectedSheet: "",
    fileType: "csv",
  };
}

describe("App", () => {
  it("switches to the workbench after browsing files and preserves credentials when editing the connection", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          files: [{ key: "folder/sample.csv", size: 128, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
        }),
      }),
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

  it("renders schema results and preview rows after processing in the workbench", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => buildProcessResponse(),
        }),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process selection/i }));

    await waitFor(() => expect(screen.getByText("Inferred schema")).toBeInTheDocument());
    expect(screen.getByRole("cell", { name: "Score" })).toBeInTheDocument();
    expect(screen.getByLabelText(/Override type for Score/i)).toHaveValue("float");
    expect(screen.getByText("Processed preview")).toBeInTheDocument();
    expect(screen.getByText(/Preview rows 1-2 of 2/i)).toBeInTheDocument();
  });

  it("resets overrides back to the detected schema after reprocessing", async () => {
    const fetchMock = vi.fn();
    fetchMock
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => buildProcessResponse(),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () =>
          buildProcessResponse({
            runId: 2,
            schema: [
              {
                column: "Score",
                inferred_type: "text",
                storage_type: "string",
                display_type: "Text",
                nullable: true,
                confidence: 0.97,
                warnings: [],
                null_token_count: 1,
                sample_values: ["90", "75"],
                allowed_overrides: ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"],
              },
            ],
            previewRows: [{ Score: "90" }, { Score: "75" }],
          }),
      });

    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process selection/i }));
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
    const previewRows = Array.from({ length: 30 }, (_, index) => ({ Index: index + 1 }));
    const processResponse = buildProcessResponse({
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
      previewRows,
    });

    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({
            files: [{ key: "sample.csv", size: 147, lastModified: "2026-04-03T00:00:00Z", format: "csv" }],
          }),
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => processResponse,
        })
        .mockResolvedValueOnce({
          ok: true,
          json: async () => ({ ...processResponse, runId: 2 }),
        }),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));
    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process selection/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 1-25 of 30/i)).toBeInTheDocument());
    expect(screen.getByRole("cell", { name: "1" })).toBeInTheDocument();
    expect(screen.queryByRole("cell", { name: "26" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Next page/i }));
    expect(screen.getByText(/Preview rows 26-30 of 30/i)).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "26" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Previous page/i }));
    expect(screen.getByText(/Preview rows 1-25 of 30/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Next page/i }));
    expect(screen.getByText(/Page 2 of 2/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Reprocess with overrides/i }));
    await waitFor(() => expect(screen.getByText(/Preview rows 1-25 of 30/i)).toBeInTheDocument());
    expect(screen.getByText(/Page 1 of 2/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Next page/i }));
    expect(screen.getByText(/Preview rows 26-30 of 30/i)).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Rows per page/i), { target: { value: "50" } });
    expect(screen.getByText(/Preview rows 1-30 of 30/i)).toBeInTheDocument();
    expect(screen.getByText(/Page 1 of 1/i)).toBeInTheDocument();
  });

  it("shows a stable empty state when browsing returns no supported files", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          files: [],
        }),
      }),
    );

    render(<App />);

    enterRequiredCredentials();
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("Processing workbench")).toBeInTheDocument());
    expect(screen.getByText(/No supported files were found for this bucket or prefix/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Process selection/i })).toBeDisabled();
  });
});
