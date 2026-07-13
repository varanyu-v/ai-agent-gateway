import os
import unittest
from unittest.mock import MagicMock, patch

from apps import observability


class LangfuseObservabilityConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        observability._LANGFUSE_PROVIDERS.clear()

    def tearDown(self) -> None:
        observability._LANGFUSE_PROVIDERS.clear()

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

    def test_langfuse_uses_a_dedicated_tracer_provider(self) -> None:
        provider = MagicMock()
        tracer = MagicMock()
        provider.get_tracer.return_value = tracer

        with (
            patch.dict(
                os.environ,
                {
                    "LANGFUSE_PUBLIC_KEY": "pk-test",
                    "LANGFUSE_SECRET_KEY": "sk-test",
                },
                clear=True,
            ),
            patch.object(observability, "TracerProvider", return_value=provider),
            patch.object(observability, "OTLPSpanExporter") as exporter,
            patch.object(observability, "BatchSpanProcessor") as processor,
        ):
            result = observability.setup_langfuse_observability("orchestrator")

        exporter.assert_called_once_with(
            endpoint="https://cloud.langfuse.com/api/public/otel/v1/traces",
            headers={
                "Authorization": "Basic cGstdGVzdDpzay10ZXN0",
                "x-langfuse-ingestion-version": "4",
            },
        )
        processor.assert_called_once_with(exporter.return_value)
        provider.add_span_processor.assert_called_once_with(processor.return_value)
        provider.get_tracer.assert_called_once_with("orchestrator.langfuse")
        self.assertIs(result, tracer)

    def test_shared_observability_does_not_read_langfuse_configuration(self) -> None:
        service_name = "tempo-only-test"
        observability._CONFIGURED_SERVICES.discard(service_name)

        with (
            patch.dict(os.environ, {"OTEL_ENABLED": "true"}, clear=True),
            patch.object(observability, "_traces_endpoint", return_value=None),
            patch.object(observability, "_metrics_endpoint", return_value=None),
            patch.object(observability, "_langfuse_traces_endpoint") as endpoint,
            patch.object(observability, "_resource"),
            patch.object(observability, "TracerProvider"),
            patch.object(observability.trace, "set_tracer_provider"),
            patch.object(observability.trace, "get_tracer_provider"),
            patch.object(observability.trace, "get_tracer"),
            patch.object(observability.metrics, "get_meter_provider"),
            patch.object(observability, "_HTTPX_INSTRUMENTED", True),
            patch.object(observability, "_ASYNCPG_INSTRUMENTED", True),
            patch.object(observability, "_LOGGING_INSTRUMENTED", True),
        ):
            observability.setup_observability(service_name)

        endpoint.assert_not_called()
        observability._CONFIGURED_SERVICES.discard(service_name)


if __name__ == "__main__":
    unittest.main()
