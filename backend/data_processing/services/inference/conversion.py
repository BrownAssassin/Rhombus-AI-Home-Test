"""Override validation, dataframe conversion, and preview serialization."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pandas as pd

from .constants import ALLOWED_OVERRIDE_TYPES, TYPE_DISPLAY_NAMES
from .parsers import normalize_scalar, parse_bool_token, parse_datetime_series, parse_decimal
from .profiles import ColumnProfile


def can_profile_convert_to(profile: ColumnProfile, target_type: str) -> bool:
    """Return whether a manual override can be applied without lossy coercion."""

    if target_type in {"text", "category"}:
        return True
    if target_type == "integer":
        return profile.integer_valid
    if target_type == "float":
        return profile.float_valid or profile.integer_valid
    if target_type == "boolean":
        return profile.boolean_valid
    if target_type in {"date", "datetime"}:
        return profile.datetime_valid
    if target_type == "complex":
        return profile.complex_valid
    return False


def validate_overrides(
    profiles: dict[str, ColumnProfile],
    schema: list[dict[str, Any]],
    overrides: dict[str, str],
) -> list[dict[str, Any]]:
    """Apply validated overrides to an inferred schema payload."""

    schema_by_column = {item["column"]: dict(item) for item in schema}
    for column, target_type in overrides.items():
        if column not in profiles:
            raise ValueError(f"Column '{column}' does not exist in the dataset.")
        if target_type not in ALLOWED_OVERRIDE_TYPES:
            raise ValueError(f"Unsupported override type '{target_type}'.")
        if not can_profile_convert_to(profiles[column], target_type):
            raise ValueError(f"Column '{column}' cannot be safely converted to '{target_type}'.")

        schema_entry = schema_by_column[column]
        schema_entry["inferred_type"] = target_type
        schema_entry["display_type"] = TYPE_DISPLAY_NAMES[target_type]
        schema_entry["storage_type"] = {
            "text": "string",
            "integer": "Int64",
            "float": "Float64",
            "boolean": "boolean",
            "date": "datetime64[ns]",
            "datetime": "datetime64[ns]",
            "category": "category",
            "complex": "object",
        }[target_type]
        if target_type in {"date", "datetime"} and profiles[column].ambiguous_datetime:
            schema_entry["warnings"] = [
                "This column was manually forced to a date-like type. Ambiguous values will use pandas defaults."
            ]

    return [schema_by_column[item["column"]] for item in schema]


def convert_series(series: pd.Series, target_type: str) -> pd.Series:
    """Convert one series to the requested logical target type."""

    normalized = series.map(normalize_scalar)

    if target_type == "text":
        return normalized.astype("string")

    if target_type == "category":
        return pd.Series(pd.Categorical(normalized), index=series.index, name=series.name)

    if target_type == "integer":
        parsed = normalized.map(lambda value: parse_decimal(value) if value is not None else None)
        if parsed.map(lambda value: value is None or value == value.to_integral_value()).all():
            return parsed.map(lambda value: int(value) if value is not None else pd.NA).astype("Int64")
        raise ValueError(f"Column '{series.name}' contains non-integer values.")

    if target_type == "float":
        parsed = normalized.map(lambda value: parse_decimal(value) if value is not None else None)
        if parsed.map(lambda value: value is None or isinstance(value, Decimal)).all():
            return parsed.map(lambda value: float(value) if value is not None else pd.NA).astype("Float64")
        raise ValueError(f"Column '{series.name}' contains non-numeric values.")

    if target_type == "boolean":
        parsed = normalized.map(lambda value: parse_bool_token(value) if value is not None else pd.NA)
        if parsed.map(lambda value: value is pd.NA or isinstance(value, bool)).all():
            return parsed.astype("boolean")
        raise ValueError(f"Column '{series.name}' contains non-boolean values.")

    if target_type == "date":
        dt_series = parse_datetime_series(normalized, series.name)
        return dt_series.dt.normalize()

    if target_type == "datetime":
        return parse_datetime_series(normalized, series.name)

    if target_type == "complex":
        return normalized.map(lambda value: complex(value) if value is not None else None)

    raise ValueError(f"Unsupported target type '{target_type}'.")


def convert_dataframe(df: pd.DataFrame, schema: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert a dataframe according to the inferred or overridden schema."""

    converted = pd.DataFrame(index=df.index)
    for item in schema:
        column = item["column"]
        converted[column] = convert_series(df[column], item["inferred_type"])
    return converted


def serialize_scalar(value: Any, *, target_type: str | None = None) -> Any:
    """Serialize pandas and numpy scalars into JSON-friendly preview values."""

    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if target_type == "date":
            return value.date().isoformat()
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except ValueError:
            pass
    if isinstance(value, complex):
        return str(value)
    return value


def dataframe_preview(
    df: pd.DataFrame,
    limit: int,
    *,
    schema: list[dict[str, Any]] | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Serialize the first preview rows for API responses and CLI output."""

    preview_df = df.head(limit)
    target_types = {item["column"]: item["inferred_type"] for item in schema or []}
    rows = []
    for _, row in preview_df.iterrows():
        rows.append(
            {
                column: serialize_scalar(value, target_type=target_types.get(column))
                for column, value in row.items()
            }
        )
    return list(preview_df.columns), rows
