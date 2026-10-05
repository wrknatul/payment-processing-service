"""Demo webhook receiver: not part of the service, only a target for trying it out.

POST /ok        always answers 200
POST /fail      always answers 500 (the message ends up in the dead letter queue)
POST /flaky     answers 500 to the first two deliveries of an event, then 200
GET  /received  lists everything received so far
"""

import logging
import os
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response

from app.services.webhooks import EVENT_ID_HEADER, SIGNATURE_HEADER, sign

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("webhook-sink")

SIGNING_SECRET = os.environ.get("WEBHOOK_SIGNING_SECRET", "dev-webhook-secret")
FLAKY_FAILURES = 2

app = FastAPI(title="Webhook sink (demo)")
received: list[dict[str, Any]] = []
deliveries: Counter[str] = Counter()


async def record(request: Request, status_code: int) -> Response:
    body = await request.body()
    event_id = request.headers.get(EVENT_ID_HEADER, "")
    signature_valid = request.headers.get(SIGNATURE_HEADER) == sign(body, SIGNING_SECRET)
    received.append(
        {
            "received_at": datetime.now(UTC).isoformat(),
            "path": request.url.path,
            "event_id": event_id,
            "response_status": status_code,
            "signature_valid": signature_valid,
            "body": body.decode(errors="replace"),
        }
    )
    logger.info(
        "%s event=%s -> %d, signature_valid=%s",
        request.url.path,
        event_id,
        status_code,
        signature_valid,
    )
    return Response(status_code=status_code)


@app.post("/ok")
async def ok(request: Request) -> Response:
    return await record(request, 200)


@app.post("/fail")
async def fail(request: Request) -> Response:
    return await record(request, 500)


@app.post("/flaky")
async def flaky(request: Request) -> Response:
    event_id = request.headers.get(EVENT_ID_HEADER, "")
    deliveries[event_id] += 1
    status_code = 500 if deliveries[event_id] <= FLAKY_FAILURES else 200
    return await record(request, status_code)


@app.get("/received")
async def list_received() -> list[dict[str, Any]]:
    return received
