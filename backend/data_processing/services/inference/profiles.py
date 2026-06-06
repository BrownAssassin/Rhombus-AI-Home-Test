"""Profile dataclasses and profile-update helpers for inference."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable

import pandas as pd

from data_processing.contracts import SchemaItem

from .parsers import (
    normalize_scalar,
    parse_bool_token,
    parse_complex_candidate,
    parse_datetime_candidate,
    parse_decimal,
)


@dataclass
class ColumnProfile:
    """Rolling evidence gathered for one column during profiling."""

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
        """Initialize mutable defaults without sharing state across profiles."""

        if self.sample_values is None:
            self.sample_values = []
        if self.unique_values is None:
            self.unique_values = set()

    @property
    def unique_count(self) -> int:
        """Return the number of unique non-null values sampled for the column."""

        return len(self.unique_values)


@dataclass
class ColumnInference:
    """Serializable schema metadata exposed to the API and frontend."""

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

    def to_dict(self) -> SchemaItem:
        """Convert the dataclass to the API payload shape."""

        return asdict(self)


def create_profiles(columns: Iterable[str]) -> dict[str, ColumnProfile]:
    """Create empty column profiles in source column order."""

    return {column: ColumnProfile(name=column) for column in columns}


def update_profiles_from_dataframe(profiles: dict[str, ColumnProfile], df: pd.DataFrame) -> None:
    """Update column profiles from one dataframe or chunk of raw string values."""

    for column in df.columns:
        profile = profiles.setdefault(column, ColumnProfile(name=column))
        for raw_value in df[column].array:
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

            if profile.integer_valid or profile.float_valid:
                decimal_value = parse_decimal(normalized)
                if decimal_value is None:
                    profile.integer_valid = False
                    profile.float_valid = False
                elif profile.integer_valid and decimal_value != decimal_value.to_integral_value():
                    profile.integer_valid = False

            if profile.boolean_valid:
                bool_value = parse_bool_token(normalized)
                if bool_value is None:
                    profile.boolean_valid = False
                elif not normalized.isdigit():
                    profile.boolean_has_alpha_tokens = True

            if profile.datetime_valid:
                datetime_valid, ambiguous, has_time = parse_datetime_candidate(normalized)
                if not datetime_valid:
                    profile.datetime_valid = False
                if ambiguous:
                    profile.ambiguous_datetime = True
                if has_time:
                    profile.has_time_component = True

            if profile.complex_valid and not parse_complex_candidate(normalized):
                profile.complex_valid = False


def profile_dataframe(df: pd.DataFrame) -> dict[str, ColumnProfile]:
    """Profile an in-memory dataframe in one pass."""

    profiles = create_profiles(df.columns)
    update_profiles_from_dataframe(profiles, df.astype("string"))
    return profiles
