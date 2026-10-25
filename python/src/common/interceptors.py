"""Server interceptors: request-ID logging and mTLS peer authorization."""

import logging
import time
import uuid
from collections.abc import Callable, Collection
from contextvars import ContextVar

import grpc
from opentelemetry import trace

REQUEST_ID_METADATA_KEY = "x-request-id"

# Health and reflection stay reachable by any CA-signed peer (probes, grpcurl).
AUTHORIZATION_EXEMPT_METHOD_PREFIXES = ("/grpc.health.v1.Health/", "/grpc.reflection.")

request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

logger = logging.getLogger(__name__)


def peer_common_names(context: grpc.ServicerContext) -> list[str]:
    return [name.decode() for name in context.auth_context().get("x509_common_name", [])]


def _wrap_handler(handler: grpc.RpcMethodHandler, wrap_behavior: Callable) -> grpc.RpcMethodHandler:
    """Applies wrap_behavior(behavior, is_streaming_response) to the handler's single behavior."""
    if handler.unary_unary:
        return handler._replace(unary_unary=wrap_behavior(handler.unary_unary, False))
    if handler.stream_unary:
        return handler._replace(stream_unary=wrap_behavior(handler.stream_unary, False))
    if handler.unary_stream:
        return handler._replace(unary_stream=wrap_behavior(handler.unary_stream, True))
    return handler._replace(stream_stream=wrap_behavior(handler.stream_stream, True))


class ServerObservabilityInterceptor(grpc.ServerInterceptor):
    """Logs method, caller, status and latency, tagged with the propagated x-request-id."""

    def __init__(self, service_name: str) -> None:
        self._service_name = service_name

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        method_name = handler_call_details.method
        incoming_metadata = dict(handler_call_details.invocation_metadata or ())
        request_id = incoming_metadata.get(REQUEST_ID_METADATA_KEY) or uuid.uuid4().hex[:12]

        def log_completion(context, start_time, default_code):
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            status_code = context.code() or default_code
            logger.info(
                "[%s] rid=%s caller=%s %s -> %s (%.1f ms)",
                self._service_name, request_id, ",".join(peer_common_names(context)) or "-",
                method_name, status_code.name, elapsed_ms,
            )

        def wrap_behavior(behavior, is_streaming_response):
            if is_streaming_response:
                def wrapped_stream(request_or_iterator, context):
                    request_id_var.set(request_id)
                    trace.get_current_span().set_attribute("request.id", request_id)
                    start_time = time.perf_counter()
                    try:
                        yield from behavior(request_or_iterator, context)
                    except Exception:
                        log_completion(context, start_time, grpc.StatusCode.UNKNOWN)
                        raise
                    log_completion(context, start_time, grpc.StatusCode.OK)

                return wrapped_stream

            def wrapped_unary(request_or_iterator, context):
                request_id_var.set(request_id)
                trace.get_current_span().set_attribute("request.id", request_id)
                start_time = time.perf_counter()
                try:
                    response = behavior(request_or_iterator, context)
                except Exception:
                    log_completion(context, start_time, grpc.StatusCode.UNKNOWN)
                    raise
                log_completion(context, start_time, grpc.StatusCode.OK)
                return response

            return wrapped_unary

        return _wrap_handler(handler, wrap_behavior)


class PeerAuthorizationInterceptor(grpc.ServerInterceptor):
    """Rejects callers whose client-certificate CN is not in allowed_callers."""

    def __init__(self, allowed_callers: Collection[str]) -> None:
        self._allowed_callers = frozenset(allowed_callers)

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None or handler_call_details.method.startswith(AUTHORIZATION_EXEMPT_METHOD_PREFIXES):
            return handler

        allowed_callers = self._allowed_callers

        def authorize(context):
            caller_names = peer_common_names(context)
            if not allowed_callers.intersection(caller_names):
                context.abort(grpc.StatusCode.PERMISSION_DENIED, f"caller {caller_names or 'unknown'} not allowed")

        def wrap_behavior(behavior, is_streaming_response):
            if is_streaming_response:
                def wrapped_stream(request_or_iterator, context):
                    authorize(context)
                    yield from behavior(request_or_iterator, context)

                return wrapped_stream

            def wrapped_unary(request_or_iterator, context):
                authorize(context)
                return behavior(request_or_iterator, context)

            return wrapped_unary

        return _wrap_handler(handler, wrap_behavior)
