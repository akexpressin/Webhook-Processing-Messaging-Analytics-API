from fastapi import FastAPI
from app.storage import init_db
import os
from app.storage import get_connection
from pydantic import BaseModel, Field, field_validator
from typing import Optional
import re
from fastapi import Request, HTTPException
from app.storage import insert_message
import hmac
import hashlib
from fastapi import Header
from fastapi import Depends
from fastapi import Query
from app.storage import list_messages, count_messages
from app.storage import get_basic_stats, get_messages_per_sender


class WebhookMessage(BaseModel):
    message_id: str = Field(..., min_length=1)
    from_: str = Field(..., alias="from")
    to: str
    ts: str
    text: Optional[str] = Field(None, max_length=4096)

    @field_validator("from_", "to")
    @classmethod
    def validate_msisdn(cls, v: str):
        if not re.match(r"^\+\d+$", v):
            raise ValueError("Invalid E.164 format")
        return v
    

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()

@app.get("/")
def root():
    return {"status": "running"}

@app.get("/health/live")
def health_live():
    return {"status": "live"}

@app.get("/health/ready")
def health_ready():
    try:
        if not os.getenv("WEBHOOK_SECRET"):
            return {"status": "not ready"}, 503

        conn = get_connection()
        conn.execute("SELECT 1")
        conn.close()

        return {"status": "ready"}
    except Exception:
        return {"status": "not ready"}, 503
    
def verify_signature(raw_body: bytes, signature: str):
    secret = os.getenv("WEBHOOK_SECRET")

    if not secret:
        return False

    computed = hmac.new(
        key=secret.encode(),
        msg=raw_body,
        digestmod=hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(computed, signature)
    
@app.post("/webhook")
async def webhook(
    request: Request,
    x_signature: str = Header(None)
):
    raw_body = await request.body()

    if not x_signature or not verify_signature(raw_body, x_signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    payload = WebhookMessage.model_validate_json(raw_body)

    insert_message(payload)
    return {"status": "ok"}

@app.get("/messages")
def get_messages(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    from_: str | None = Query(None, alias="from"),
    since: str | None = None,
    q: str | None = None,
):
    rows = list_messages(
        limit=limit,
        offset=offset,
        from_msisdn=from_,
        since=since,
        q=q,
    )

    total = count_messages(
        from_msisdn=from_,
        since=since,
        q=q,
    )

    data = [
        {
            "message_id": r["message_id"],
            "from": r["from_msisdn"],
            "to": r["to_msisdn"],
            "ts": r["ts"],
            "text": r["text"],
        }
        for r in rows
    ]

    return {
        "data": data,
        "total": total,
        "limit": limit,
        "offset": offset,
    }

@app.get("/stats")
def get_stats():
    total_messages, senders_count, first_ts, last_ts = get_basic_stats()
    messages_per_sender = get_messages_per_sender()

    return {
        "total_messages": total_messages,
        "senders_count": senders_count,
        "messages_per_sender": messages_per_sender,
        "first_message_ts": first_ts,
        "last_message_ts": last_ts,
    }


