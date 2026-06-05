"""Inference rules and confidence heuristics derived from collected profiles."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .constants import (
    ALLOWED_OVERRIDE_TYPES,
    LARGE_SAMPLE_CATEGORY_MAX_RATIO,
    LARGE_SAMPLE_CATEGORY_MAX_UNIQUE,
    LARGE_SAMPLE_CATEGORY_MIN_ROWS,
    SMALL_SAMPLE_CATEGORY_MAX_RATIO,
    SMALL_SAMPLE_CATEGORY_MAX_UNIQUE,
    SMALL_SAMPLE_CATEGORY_MIN_ROWS,
    TYPE_DISPLAY_NAMES,
)
from .profiles import ColumnInference, ColumnProfile, profile_dataframe


def build_column_inference(profile: ColumnProfile) -> ColumnInference:
    """Translate a column profile into the user-facing inferred schema entry."""

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
        qualifies_large_sample = (
            profile.non_null_count >= LARGE_SAMPLE_CATEGORY_MIN_ROWS
            and profile.unique_count <= LARGE_SAMPLE_CATEGORY_MAX_UNIQUE
            and unique_ratio <= LARGE_SAMPLE_CATEGORY_MAX_RATIO
        )
        qualifies_small_sample = (
            profile.non_null_count >= SMALL_SAMPLE_CATEGORY_MIN_ROWS
            and profile.unique_count <= SMALL_SAMPLE_CATEGORY_MAX_UNIQUE
            and unique_ratio <= SMALL_SAMPLE_CATEGORY_MAX_RATIO
        )
        if qualifies_large_sample or qualifies_small_sample:
            inferred_type = "category"
            confidence = 0.76 if qualifies_large_sample else 0.64
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
    """Infer the full schema from already-collected column profiles."""

    return [build_column_inference(profile).to_dict() for profile in profiles.values()]


def infer_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Infer a schema directly from an in-memory dataframe."""

    return infer_profiles(profile_dataframe(df))
