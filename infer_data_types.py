"""CLI wrapper around the shared local-file processing service."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent
# The shared services live in the Django app package under ./backend.
sys.path.insert(0, str(REPO_ROOT / "backend"))

from data_processing.services.processing import ProcessingServiceError, process_local_file


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line interface for local smoke testing."""

    parser = argparse.ArgumentParser(description="Infer column types for a local CSV or Excel file.")
    parser.add_argument(
        "path",
        nargs="?",
        default="examples/sample_data.csv",
        help="Path to a CSV, XLS, or XLSX file.",
    )
    parser.add_argument("--sheet-name", default="", help="Optional Excel sheet name.")
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=10,
        help="Number of preview rows to print after conversion.",
    )
    return parser


def print_schema(result: dict) -> None:
    """Print the inferred schema in a compact human-readable format."""

    print(f"File: {result['objectKey']}")
    print(f"Rows profiled: {result['rowCount']}")
    print("Inferred schema:")
    for item in result["schema"]:
        warning_suffix = f" Warnings: {'; '.join(item['warnings'])}" if item["warnings"] else ""
        print(
            f"  - {item['column']}: {item['display_type']} "
            f"(nullable={item['nullable']}, confidence={item['confidence']:.2f})."
            f"{warning_suffix}"
        )


def print_preview(result: dict) -> None:
    """Print preview rows using the processed column order."""

    if not result["previewRows"]:
        print("No preview rows available.")
        return

    print("\nPreview rows:")
    columns = result["previewColumns"]
    print(" | ".join(columns))
    for row in result["previewRows"]:
        print(" | ".join("" if row[column] is None else str(row[column]) for column in columns))


def main() -> int:
    """Run the CLI and return a shell-friendly exit code."""

    parser = build_parser()
    args = parser.parse_args()

    try:
        result = process_local_file(
            args.path,
            sheet_name=args.sheet_name,
            preview_row_limit=args.preview_rows,
        )
    except ProcessingServiceError as exc:
        print(f"Error: {exc}")
        return 1

    print_schema(result)
    if result["warnings"]:
        print("\nDataset warnings:")
        for warning in result["warnings"]:
            print(f"  - {warning}")
    print_preview(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
