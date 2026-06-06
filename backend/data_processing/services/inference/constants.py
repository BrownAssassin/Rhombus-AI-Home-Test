"""Stable constants and heuristics used by the inference package."""

from __future__ import annotations

import re


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
SMALL_SAMPLE_CATEGORY_MIN_ROWS = 5
SMALL_SAMPLE_CATEGORY_MAX_UNIQUE = 5
SMALL_SAMPLE_CATEGORY_MAX_RATIO = 0.4
LARGE_SAMPLE_CATEGORY_MIN_ROWS = 20
LARGE_SAMPLE_CATEGORY_MAX_UNIQUE = 50
LARGE_SAMPLE_CATEGORY_MAX_RATIO = 0.2
