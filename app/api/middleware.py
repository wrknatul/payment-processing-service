import uuid

from starlette.datastructures import Headers, MutableHeaders
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

TRACE_HEADER = "X-Request-ID"
MAX_TRACE_ID_LENGTH = 128


class TraceIdMiddleware:
    """Gives every request a trace id: the caller's X-Request-ID or a fresh UUID.

    The id is kept in the request state for error bodies and logs and echoed in the response.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = Headers(scope=scope).get(TRACE_HEADER, "")
        trace_id = incoming if 0 < len(incoming) <= MAX_TRACE_ID_LENGTH else str(uuid.uuid4())
        scope.setdefault("state", {})["trace_id"] = trace_id

        async def send_with_trace_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[TRACE_HEADER] = trace_id
            await send(message)

        await self.app(scope, receive, send_with_trace_id)


def get_trace_id(request: Request) -> str:
    trace_id: str | None = getattr(request.state, "trace_id", None)
    return trace_id or str(uuid.uuid4())
