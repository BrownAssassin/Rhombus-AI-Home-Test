"""Shared fixtures and fakes for backend regression tests."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace


def credentials_payload() -> dict[str, str]:
    """Return a reusable API/request credential payload."""

    return {
        "access_key_id": "access",
        "secret_access_key": "secret",
        "session_token": "",
        "region": "ap-southeast-2",
        "bucket": "demo-bucket",
        "prefix": "incoming/",
    }


def sample_schema_item(*, column: str = "Score") -> dict[str, object]:
    """Return one representative schema item for tests that need preview context."""

    return {
        "column": column,
        "inferred_type": "integer",
        "storage_type": "Int64",
        "display_type": "Integer",
        "nullable": False,
        "confidence": 0.98,
        "warnings": [],
        "null_token_count": 0,
        "sample_values": ["90", "75"],
        "allowed_overrides": [
            "text",
            "integer",
            "float",
            "boolean",
            "date",
            "datetime",
            "category",
            "complex",
        ],
    }


class FakePaginator:
    """Minimal paginator stub for list_objects_v2 tests."""

    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return self.pages


class FakeS3Client:
    """Small S3 client stub for staging, listing, and preview tests."""

    def __init__(self, *, objects=None, pages=None):
        self.objects = objects or {}
        self.pages = pages or []
        self.head_calls = 0
        self.download_calls = 0

    def get_paginator(self, name: str):
        if name != "list_objects_v2":
            raise AssertionError(f"Unexpected paginator requested: {name}")
        return FakePaginator(self.pages)

    def head_object(self, Bucket: str, Key: str):
        self.head_calls += 1
        return {"ContentLength": len(self.objects[Key]), "ETag": '"demo-etag"'}

    def download_fileobj(self, Bucket: str, Key: str, fileobj):
        self.download_calls += 1
        fileobj.write(self.objects[Key])


class FakeSparkField:
    """Minimal Spark schema field stub for schema mapping tests."""

    def __init__(self, name: str, spark_type: str, *, nullable: bool = True):
        self.name = name
        self.nullable = nullable
        self.dataType = SimpleNamespace(simpleString=lambda: spark_type)


class FakeSparkRow:
    """Small row stub exposing Spark's asDict interface."""

    def __init__(self, payload):
        self.payload = payload

    def asDict(self, recursive: bool = True):
        return self.payload


class FakeSparkZippedRows:
    """Chainable zipWithIndex result for preview slicing tests."""

    def __init__(self, items):
        self.items = items

    def filter(self, predicate):
        self.items = [item for item in self.items if predicate(item)]
        return self

    def map(self, mapper):
        self.items = [mapper(item) for item in self.items]
        return self

    def collect(self):
        return self.items


class FakeSparkRdd:
    """Minimal RDD stub that supports zipWithIndex()."""

    def __init__(self, rows):
        self.rows = rows

    def zipWithIndex(self):
        return FakeSparkZippedRows([(FakeSparkRow(row), index) for index, row in enumerate(self.rows)])


class FakeSparkDataFrame:
    """Simplified Spark DataFrame stub for comparison tests."""

    def __init__(self, *, rows, fields):
        self._rows = rows
        self.columns = list(rows[0].keys()) if rows else []
        self.schema = SimpleNamespace(fields=fields)
        self.rdd = FakeSparkRdd(rows)

    def count(self):
        return len(self._rows)


class FakeSparkReader:
    """Spark reader stub that records CSV read invocations."""

    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.csv_calls = 0

    def option(self, *_args, **_kwargs):
        return self

    def csv(self, _path):
        self.csv_calls += 1
        return self.dataframe


class FakeSparkSession:
    """Minimal Spark session stub with a single reusable reader."""

    def __init__(self, dataframe):
        self.read = FakeSparkReader(dataframe)
        self.stopped = False

    def stop(self):
        self.stopped = True


def spark_dataframe_fixture():
    """Build one representative Spark DataFrame fixture for comparison tests."""

    dataframe = FakeSparkDataFrame(
        rows=[
            {
                "Date": date(2026, 6, 5),
                "OccurredAt": datetime(2026, 6, 5, 12, 30),
                "Amount": Decimal("12.50"),
                "Score": 90,
            }
        ],
        fields=[
            FakeSparkField("Date", "date"),
            FakeSparkField("OccurredAt", "timestamp"),
            FakeSparkField("Amount", "decimal"),
            FakeSparkField("Score", "integer", nullable=False),
        ],
    )
    spark = FakeSparkSession(dataframe)
    return dataframe, spark, nullcontext(SimpleNamespace(path=Path("fake.csv")))


SAMPLE_LAST_MODIFIED = datetime(2026, 4, 4, tzinfo=timezone.utc)
