import unittest
from unittest.mock import MagicMock, patch

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    TraceState,
)

from apps.orchestrator import main as orchestrator


TRACE_ID = 0x1234567890ABCDEF1234567890ABCDEF
TEMPO_SPAN_ID = 0x1234567890ABCDEF
LANGFUSE_ROOT_SPAN_ID = 0xFEDCBA0987654321


def span_context(span_id: int) -> SpanContext:
    return SpanContext(
        trace_id=TRACE_ID,
        span_id=span_id,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=TraceState(),
    )


class OrchestratorLangfuseTraceTests(unittest.TestCase):
    def setUp(self) -> None:
        orchestrator.LANGFUSE_RUN_TRACES.clear()
        orchestrator.LANGFUSE_TOOL_TRACES.clear()

    def tearDown(self) -> None:
        orchestrator.LANGFUSE_RUN_TRACES.clear()
        orchestrator.LANGFUSE_TOOL_TRACES.clear()

    def test_generation_is_a_child_of_agent_trace_with_tempo_correlation(self) -> None:
        root_span = MagicMock()
        root_span.get_span_context.return_value = span_context(LANGFUSE_ROOT_SPAN_ID)
        generation_span = MagicMock()
        langfuse_tracer = MagicMock()
        langfuse_tracer.start_span.side_effect = [root_span, generation_span]

        tempo_context = otel_trace.set_span_in_context(
            NonRecordingSpan(span_context(TEMPO_SPAN_ID)),
        )
        token = otel_context.attach(tempo_context)
        try:
            with patch.object(orchestrator, "langfuse_tracer", langfuse_tracer):
                with orchestrator.langfuse_agent_trace(
                    request_id="request-1",
                    session_id="thread-1",
                    tenant_id="tenant-1",
                    user_id="user-1",
                    agent_id="world-agent",
                    workflow="world",
                    message="show the largest cities",
                ):
                    with orchestrator.langfuse_generation_span(
                        "world",
                        "show the largest cities",
                        {
                            "request_id": "request-1",
                            "tenant_id": "tenant-1",
                            "user_id": "user-1",
                            "agent_id": "world-agent",
                        },
                    ):
                        pass
        finally:
            otel_context.detach(token)

        root_call, generation_call = langfuse_tracer.start_span.call_args_list
        root_parent = otel_trace.get_current_span(root_call.kwargs["context"])
        generation_parent = otel_trace.get_current_span(
            generation_call.kwargs["context"],
        )
        self.assertEqual(root_parent.get_span_context().span_id, TEMPO_SPAN_ID)
        self.assertEqual(
            generation_parent.get_span_context().span_id,
            LANGFUSE_ROOT_SPAN_ID,
        )

        root_attributes = root_call.kwargs["attributes"]
        generation_attributes = generation_call.kwargs["attributes"]
        self.assertEqual(
            root_attributes["langfuse.trace.metadata.tempo_trace_id"],
            f"{TRACE_ID:032x}",
        )
        self.assertEqual(root_attributes["langfuse.session.id"], "thread-1")
        self.assertEqual(root_attributes["langfuse.observation.type"], "span")
        self.assertEqual(
            generation_attributes["langfuse.observation.type"],
            "generation",
        )
        self.assertEqual(
            generation_attributes["langfuse.trace.input"],
            root_attributes["langfuse.trace.input"],
        )
        root_span.end.assert_called_once_with()
        generation_span.end.assert_called_once_with()

    def test_tool_trace_collects_request_result_and_final_output(self) -> None:
        root_span = MagicMock()
        root_span.get_span_context.return_value = span_context(LANGFUSE_ROOT_SPAN_ID)
        tool_span = MagicMock()
        langfuse_tracer = MagicMock()
        langfuse_tracer.start_span.side_effect = [root_span, tool_span]

        tempo_context = otel_trace.set_span_in_context(
            NonRecordingSpan(span_context(TEMPO_SPAN_ID)),
        )
        token = otel_context.attach(tempo_context)
        try:
            with patch.object(orchestrator, "langfuse_tracer", langfuse_tracer):
                with orchestrator.langfuse_agent_trace(
                    request_id="request-2",
                    session_id="request-2",
                    tenant_id="tenant-1",
                    user_id="user-1",
                    agent_id="world-agent",
                    workflow="world",
                    message="show city data",
                ):
                    orchestrator.start_langfuse_tool_trace(
                        {
                            "request_id": "request-2",
                            "workflow": "world",
                            "tool": "sql",
                            "tool_call_id": "request-2:sql:1",
                            "input": {"database": "world", "sql": "select 1"},
                        },
                    )

                self.assertFalse(tool_span.end.called)
                orchestrator.finish_langfuse_tool_trace(
                    {
                        "request_id": "request-2",
                        "workflow": "world",
                        "tool": "sql",
                        "tool_call_id": "request-2:sql:1",
                        "status": "completed",
                        "result": {"rows": [{"value": 1}]},
                    },
                    "SQL tool completed with 1 row(s).",
                )
        finally:
            otel_context.detach(token)

        tool_call = langfuse_tracer.start_span.call_args_list[1]
        tool_attributes = tool_call.kwargs["attributes"]
        self.assertEqual(tool_attributes["langfuse.observation.metadata.kind"], "tool")
        self.assertIn("select 1", tool_attributes["langfuse.observation.input"])

        result_attributes = tool_span.set_attributes.call_args.args[0]
        self.assertIn("completed", result_attributes["langfuse.trace.output"])
        self.assertIn("rows", result_attributes["langfuse.observation.output"])
        tool_span.end.assert_called_once_with()
        self.assertNotIn("request-2", orchestrator.LANGFUSE_RUN_TRACES)
        self.assertNotIn(
            "request-2:sql:1",
            orchestrator.LANGFUSE_TOOL_TRACES,
        )


if __name__ == "__main__":
    unittest.main()
