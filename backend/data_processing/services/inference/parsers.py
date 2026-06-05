"""Scalar normalization and parsing helpers for type inference and conversion."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import warnings
from typing import Any

import pandas as pd

from .constants import (
    AMBIGUOUS_DATE_RE,
    BOOL_FALSE_TOKENS,
    BOOL_TRUE_TOKENS,
    DATE_HINT_RE,
    GROUPED_NUMBER_RE,
    NULL_TOKENS,
    TIME_HINT_RE,
)


def normalize_scalar(value: Any) -> str | None:
    """Normalize scalars to stripped strings while honoring known null tokens."""

    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if text.casefold() in NULL_TOKENS:
        return None
    return text


def normalize_numeric_text(value: str) -> str:
    """Remove grouping separators from well-formed numeric strings."""

    if GROUPED_NUMBER_RE.match(value):
        return value.replace(",", "")
    return value


def parse_decimal(value: str) -> Decimal | None:
    """Parse a numeric string into Decimal without losing integer checks."""

    cleaned = normalize_numeric_text(value)
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_bool_token(value: str) -> bool | None:
    """Recognize the supported boolean tokens."""

    lowered = value.casefold()
    if lowered in BOOL_TRUE_TOKENS:
        return True
    if lowered in BOOL_FALSE_TOKENS:
        return False
    return None


def parse_datetime_candidate(value: str) -> tuple[bool, bool, bool]:
    """Detect parseable datetime text and flag ambiguous short dates."""

    if not DATE_HINT_RE.search(value):
        return False, False, False

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return False, False, False

    ambiguous = False
    match = AMBIGUOUS_DATE_RE.match(value)
    if match:
        first = int(match.group(1))
        second = int(match.group(2))
        if first <= 12 and second <= 12:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                month_first = pd.to_datetime(value, errors="coerce", dayfirst=False)
                day_first = pd.to_datetime(value, errors="coerce", dayfirst=True)
            if not pd.isna(month_first) and not pd.isna(day_first):
                ambiguous = month_first != day_first

    return True, ambiguous, bool(TIME_HINT_RE.search(value))


def parse_complex_candidate(value: str) -> bool:
    """Return whether a value cleanly parses as a complex number literal."""

    if "j" not in value.casefold():
        return False
    try:
        complex(value)
    except ValueError:
        return False
    return True


def parse_datetime_series(normalized: pd.Series, series_name: str) -> pd.Series:
    """Parse a normalized series as datetimes and reject invalid values."""

    dt_series = pd.to_datetime(normalized, errors="coerce")
    invalid = normalized.notna() & dt_series.isna()
    if invalid.any():
        raise ValueError(f"Column '{series_name}' contains values that are not valid dates.")
    return dt_series
