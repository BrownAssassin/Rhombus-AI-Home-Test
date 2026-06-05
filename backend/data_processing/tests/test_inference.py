"""Regression tests for the type inference service."""

from django.test import SimpleTestCase
import pandas as pd

from data_processing.services.inference import infer_dataframe, infer_profiles, profile_dataframe, validate_overrides
from data_processing.services.inference.parsers import normalize_scalar, parse_datetime_candidate


class InferenceServiceTests(SimpleTestCase):
    """Verify the most important inference and override edge cases."""

    def test_infer_dataframe_detects_boolean_category_and_ambiguous_dates(self) -> None:
        """Detect booleans, low-cardinality strings, and ambiguous dates correctly."""

        df = pd.DataFrame(
            {
                "is_active": ["yes", "no"] * 10,
                "event_date": ["01/02/2020", "02/03/2020"] * 10,
                "segment": ["enterprise"] * 12 + ["self-serve"] * 8,
            }
        )

        schema = {item["column"]: item for item in infer_dataframe(df)}

        self.assertEqual(schema["is_active"]["inferred_type"], "boolean")
        self.assertEqual(schema["segment"]["inferred_type"], "category")
        self.assertEqual(schema["event_date"]["inferred_type"], "text")
        self.assertIn("ambiguous", schema["event_date"]["warnings"][0].lower())

    def test_infer_dataframe_relaxes_category_rule_for_small_repeated_labels(self) -> None:
        """Allow obviously repeated tiny label sets to surface as categories."""

        df = pd.DataFrame(
            {
                "grade": ["A", "B", "A", "B", "A"],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            }
        )

        schema = {item["column"]: item for item in infer_dataframe(df)}

        self.assertEqual(schema["grade"]["inferred_type"], "category")
        self.assertEqual(schema["name"]["inferred_type"], "text")

    def test_validate_overrides_rejects_unsafe_integer_conversion(self) -> None:
        """Reject overrides that would silently coerce mixed text and numbers."""

        df = pd.DataFrame({"mixed": ["1", "two", "3"]})

        profiles = profile_dataframe(df)
        schema = infer_profiles(profiles)

        with self.assertRaisesMessage(ValueError, "cannot be safely converted to 'integer'"):
            validate_overrides(profiles, schema, {"mixed": "integer"})

    def test_normalize_scalar_treats_known_null_tokens_as_missing(self) -> None:
        """Normalize null-like tokens consistently across profiling and conversion."""

        self.assertIsNone(normalize_scalar(" Not Available "))

    def test_parse_datetime_candidate_flags_ambiguous_short_dates(self) -> None:
        """Keep locale-sensitive short dates visible to the inference rules."""

        self.assertEqual(parse_datetime_candidate("01/02/2020"), (True, True, False))
