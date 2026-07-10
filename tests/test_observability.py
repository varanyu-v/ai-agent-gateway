import os
import unittest
from unittest.mock import patch

from apps import observability


class LangfuseObservabilityConfigTests(unittest.TestCase):
    def test_langfuse_is_disabled_without_keys(self) -> None:
        with patch.dict(os.environ, {"LANGFUSE_ENABLED": "true"}, clear=True):
            self.assertFalse(observability._langfuse_configured())
            self.assertIsNone(observability._langfuse_traces_endpoint())
            self.assertIsNone(observability._langfuse_headers())

    def test_langfuse_headers_and_default_endpoint_use_credentials(self) -> None:
        with patch.dict(
            os.environ,
            {
                "LANGFUSE_PUBLIC_KEY": "pk-test",
                "LANGFUSE_SECRET_KEY": "sk-test",
            },
            clear=True,
        ):
            self.assertTrue(observability._langfuse_configured())
            self.assertEqual(
                observability._langfuse_traces_endpoint(),
                "https://cloud.langfuse.com/api/public/otel/v1/traces",
            )
            self.assertEqual(
                observability._langfuse_headers(),
                {
                    "Authorization": "Basic cGstdGVzdDpzay10ZXN0",
                    "x-langfuse-ingestion-version": "4",
                },
            )

    def test_langfuse_endpoint_can_target_another_region(self) -> None:
        with patch.dict(
            os.environ,
            {
                "LANGFUSE_PUBLIC_KEY": "pk-test",
                "LANGFUSE_SECRET_KEY": "sk-test",
                "LANGFUSE_BASE_URL": "https://us.cloud.langfuse.com",
            },
            clear=True,
        ):
            self.assertEqual(
                observability._langfuse_traces_endpoint(),
                "https://us.cloud.langfuse.com/api/public/otel/v1/traces",
            )


if __name__ == "__main__":
    unittest.main()
