import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./App";


afterEach(() => {
  vi.restoreAllMocks();
});


describe("App", () => {
  it("loads S3 files after credentials are entered", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          files: [
            { key: "folder/sample.csv", size: 128, lastModified: "2026-04-03T00:00:00Z", format: "csv" },
          ],
        }),
      }),
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText(/Access key ID/i), { target: { value: "AKIA123" } });
    fireEvent.change(screen.getByLabelText(/Secret access key/i), { target: { value: "secret" } });
    fireEvent.change(screen.getByLabelText(/^Bucket$/i), { target: { value: "demo-bucket" } });
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("folder/sample.csv")).toBeInTheDocument());
  });

  it("renders schema results after processing", async () => {
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
          json: async () => ({
            runId: 1,
            rowCount: 5,
            schema: [
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
            previewColumns: ["Score"],
            previewRows: [{ Score: 90 }, { Score: 75 }],
            warnings: [],
            processingMetadata: { durationMs: 11.2, previewRowLimit: 100, chunkSize: 5000 },
            selectedSheet: "",
            fileType: "csv",
          }),
        }),
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText(/Access key ID/i), { target: { value: "AKIA123" } });
    fireEvent.change(screen.getByLabelText(/Secret access key/i), { target: { value: "secret" } });
    fireEvent.change(screen.getByLabelText(/^Bucket$/i), { target: { value: "demo-bucket" } });
    fireEvent.click(screen.getByRole("button", { name: /Browse files/i }));

    await waitFor(() => expect(screen.getByText("sample.csv")).toBeInTheDocument());

    fireEvent.click(screen.getByRole("button", { name: /Process selection/i }));

    await waitFor(() => expect(screen.getByText("Inferred schema")).toBeInTheDocument());
    expect(screen.getByText("Score")).toBeInTheDocument();
    expect(screen.getByDisplayValue("Float")).toBeInTheDocument();
  });
});

