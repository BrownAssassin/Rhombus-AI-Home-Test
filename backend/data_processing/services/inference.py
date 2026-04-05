from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Iterable

import pandas as pd


NULL_TOKENS = {
    "",
    "na",
    "n/a",
    "null",
    "none",
    "not available",
    "nan",
}
BOOL_TRUE_TOKENS = {"true", "t", "yes", "y", "1"}
BOOL_FALSE_TOKENS = {"false", "f", "no", "n", "0"}
ALLOWED_OVERRIDE_TYPES = ["text", "integer", "float", "boolean", "date", "datetime", "category", "complex"]
TYPE_DISPLAY_NAMES = {
    "text": "Text",
    "integer": "Integer",
    "float": "Float",
    "boolean": "Boolean",
    "date": "Date",
    "datetime": "DateTime",
    "category": "Category",
    "complex": "Complex",
}
AMBIGUOUS_DATE_RE = re.compile(r"^\s*(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})(?:\D.*)?$")
DATE_HINT_RE = re.compile(
    r"(^\s*\d{1,4}[/-]\d{1,2}[/-]\d{1,4})|([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4})|(\d{1,2}:\d{2})"
)
TIME_HINT_RE = re.compile(r"(\d{1,2}:\d{2})|(T\d{1,2}:\d{2})")
GROUPED_NUMBER_RE = re.compile(r"^[+-]?\d{1,3}(,\d{3})+(\.\d+)?([eE][+-]?\d+)?$")


@dataclass
class ColumnProfile:
    name: str
    total_count: int = 0
    non_null_count: int = 0
    null_token_count: int = 0
    integer_valid: bool = True
    float_valid: bool = True
    boolean_valid: bool = True
    boolean_has_alpha_tokens: bool = False
    datetime_valid: bool = True
    ambiguous_datetime: bool = False
    has_time_component: bool = False
    complex_valid: bool = True
    sample_values: list[str] | None = None
    unique_values: set[str] | None = None

    def __post_init__(self) -> None:
        if self.sample_values is None:
            self.sample_values = []
        if self.unique_values is None:
            self.unique_values = set()

    @property
    def unique_count(self) -> int:
        return len(self.unique_values)


@dataclass
class ColumnInference:
    column: str
    inferred_type: str
    storage_type: str
    display_type: str
    nullable: bool
    confidence: float
    warnings: list[str]
    null_token_count: int
    sample_values: list[str]
    allowed_overrides: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_scalar(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if text.casefold() in NULL_TOKENS:
        return None
    return text


def normalize_numeric_text(value: str) -> str:
    if GROUPED_NUMBER_RE.match(value):
        return value.replace(",", "")
    return value


def parse_decimal(value: str) -> Decimal | None:
    cleaned = normalize_numeric_text(value)
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_bool_token(value: str) -> bool | None:
    lowered = value.casefold()
    if lowered in BOOL_TRUE_TOKENS:
        return True
    if lowered in BOOL_FALSE_TOKENS:
        return False
    return None


def parse_datetime_candidate(value: str) -> tuple[bool, bool, bool]:
    if not DATE_HINT_RE.search(value):
        return False, False, False

    series = pd.Series([value], dtype="string")
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.isna().all():
        return False, False, False

    ambiguous = False
    match = AMBIGUOUS_DATE_RE.match(value)
    if match:
        first = int(match.group(1))
        second = int(match.group(2))
        if first <= 12 and second <= 12:
            # Keep locale-sensitive short dates as text unless one ordering wins
            # decisively, otherwise pandas can silently reinterpret the data.
            month_first = pd.to_datetime(series, errors="coerce", dayfirst=False)
            day_first = pd.to_datetime(series, errors="coerce", dayfirst=True)
            if not month_first.isna().all() and not day_first.isna().all():
                ambiguous = month_first.iloc[0] != day_first.iloc[0]

    return True, ambiguous, bool(TIME_HINT_RE.search(value))


def parse_complex_candidate(value: str) -> bool:
    if "j" not in value.casefold():
        return False
    try:
        complex(value)
    except ValueError:
        return False
    return True


def create_profiles(columns: Iterable[str]) -> dict[str, ColumnProfile]:
    return {column: ColumnProfile(name=column) for column in columns}


def update_profiles_from_dataframe(profiles: dict[str, ColumnProfile], df: pd.DataFrame) -> None:
    for column in df.columns:
        profile = profiles.setdefault(column, ColumnProfile(name=column))
        for raw_value in df[column].tolist():
            profile.total_count += 1
            normalized = normalize_scalar(raw_value)
            if normalized is None:
                profile.null_token_count += 1
                continue

            profile.non_null_count += 1
            if normalized not in profile.sample_values and len(profile.sample_values) < 5:
                profile.sample_values.append(normalized)
            if len(profile.unique_values) < 51:
                profile.unique_values.add(normalized)

            decimal_value = parse_decimal(normalized)
            if decimal_value is None:
                profile.integer_valid = False
                profile.float_valid = False
            elif decimal_value != decimal_value.to_integral_value():
                profile.integer_valid = False

            bool_value = parse_bool_token(normalized)
            if bool_value is None:
                profile.boolean_valid = False
            elif not normalized.isdigit():
                profile.boolean_has_alpha_tokens = True

            datetime_valid, ambiguous, has_time = parse_datetime_candidate(normalized)
            if not datetime_valid:
                profile.datetime_valid = False
            if ambiguous:
                profile.ambiguous_datetime = True
            if has_time:
                profile.has_time_component = True

            if not parse_complex_candidate(normalized):
                profile.complex_valid = False


def build_column_inference(profile: ColumnProfile) -> ColumnInference:
    warnings: list[str] = []
    inferred_type = "text"
    confidence = 0.45
    storage_type = "string"

    if profile.non_null_count == 0:
        warnings.append("All values were empty or matched recognized null tokens.")
        confidence = 0.1
    elif profile.boolean_valid and profile.boolean_has_alpha_tokens:
        inferred_type = "boolean"
        confidence = 0.96
        storage_type = "boolean"
    elif profile.integer_valid:
        inferred_type = "integer"
        confidence = 0.98
        storage_type = "Int64"
    elif profile.float_valid:
        inferred_type = "float"
        confidence = 0.97
        storage_type = "Float64"
    elif profile.datetime_valid and not profile.ambiguous_datetime:
        inferred_type = "datetime" if profile.has_time_component else "date"
        confidence = 0.92
        storage_type = "datetime64[ns]"
    elif profile.datetime_valid and profile.ambiguous_datetime:
        warnings.append("Values look date-like but are ambiguous, so the column was kept as text.")
        confidence = 0.3
    elif profile.complex_valid and profile.non_null_count > 0:
        inferred_type = "complex"
        confidence = 0.8
        storage_type = "object"
    else:
        unique_ratio = profile.unique_count / profile.non_null_count if profile.non_null_count else 1
        # Small samples often look categorical by accident, so only promote
        # strings when cardinality stays low across a reasonably sized column.
        if profile.non_null_count >= 20 and profile.unique_count <= 50 and unique_ratio <= 0.2:
            inferred_type = "category"
            confidence = 0.76
            storage_type = "category"

    return ColumnInference(
        column=profile.name,
        inferred_type=inferred_type,
        storage_type=storage_type,
        display_type=TYPE_DISPLAY_NAMES[inferred_type],
        nullable=profile.null_token_count > 0,
        confidence=confidence,
        warnings=warnings,
        null_token_count=profile.null_token_count,
        sample_values=profile.sample_values,
        allowed_overrides=ALLOWED_OVERRIDE_TYPES,
    )


def infer_profiles(profiles: dict[str, ColumnProfile]) -> list[dict[str, Any]]:
    return [build_column_inference(profile).to_dict() for profile in profiles.values()]


def profile_dataframe(df: pd.DataFrame) -> dict[str, ColumnProfile]:
    profiles = create_profiles(df.columns)
    update_profiles_from_dataframe(profiles, df.astype("string"))
    return profiles


def infer_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    return infer_profiles(profile_dataframe(df))


def can_profile_convert_to(profile: ColumnProfile, target_type: str) -> bool:
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
    schema_by_column = {item["column"]: dict(item) for item in schema}
    for column, target_type in overrides.items():
        if column not in profiles:
            raise ValueError(f"Column '{column}' does not exist in the dataset.")
        if target_type not in ALLOWED_OVERRIDE_TYPES:
            raise ValueError(f"Unsupported override type '{target_type}'.")
        # Overrides are intentionally conservative so a manual selection cannot
        # hide mixed data quality issues behind a lossy coercion.
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

    if target_type in {"date", "datetime"}:
        dt_series = pd.to_datetime(normalized, errors="coerce")
        invalid = normalized.notna() & dt_series.isna()
        if invalid.any():
            raise ValueError(f"Column '{series.name}' contains values that are not valid dates.")
        return dt_series

    if target_type == "complex":
        parsed = normalized.map(lambda value: complex(value) if value is not None else None)
        return parsed

    raise ValueError(f"Unsupported target type '{target_type}'.")


def convert_dataframe(df: pd.DataFrame, schema: list[dict[str, Any]]) -> pd.DataFrame:
    converted = pd.DataFrame(index=df.index)
    for item in schema:
        column = item["column"]
        converted[column] = convert_series(df[column], item["inferred_type"])
    return converted


def serialize_scalar(value: Any) -> Any:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except ValueError:
            pass
    if isinstance(value, complex):
        return str(value)
    return value


def dataframe_preview(df: pd.DataFrame, limit: int) -> tuple[list[str], list[dict[str, Any]]]:
    preview_df = df.head(limit)
    rows = []
    for _, row in preview_df.iterrows():
        rows.append({column: serialize_scalar(value) for column, value in row.items()})
    return list(preview_df.columns), rows
