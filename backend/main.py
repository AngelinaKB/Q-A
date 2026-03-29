from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import settings
from backend.llm import generate_sql, LLMError
from backend.db import execute_query, DBError, QueryTimeoutError
from backend.validator import validate_sql
from backend.models import AskRequest, AskResponse


app = FastAPI(
    title="Ops Q&A API",
    description="Natural language → SQL → Snowflake for the operations team.",
    version="0.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.cors_allow_origin],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)


# ---------- Routes ----------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):

    # Step 1 — Generate SQL
    try:
        sql = generate_sql(req.question)
    except LLMError as e:
        return AskResponse(
            status="error",
            error_type="LLM_ERROR",
            error=str(e),
        )

    print(f"\n[ask] user={req.user_id} question={req.question!r}")
    print(f"[ask] generated SQL:\n{sql}\n")

    # Step 2 — Validate SQL (Phase 3)
    # Per design doc §9: no retry on validation failure — hard stop.
    result = validate_sql(sql)
    if not result:
        print(f"[ask] validation FAILED: {result.error_type} — {result.message}")
        return AskResponse(
            status="error",
            error_type=result.error_type,
            sql=sql,
            error=result.message,
        )

    print("[ask] validation PASSED")

    # Step 3 — Execute against Snowflake
    try:
        rows = execute_query(sql)
    except QueryTimeoutError as e:
        return AskResponse(
            status="error",
            error_type="QUERY_TIMEOUT",
            sql=sql,
            error=str(e),
        )
    except DBError as e:
        return AskResponse(
            status="error",
            error_type="DB_ERROR",
            sql=sql,
            error=str(e),
        )

    print(f"[ask] returned {len(rows)} rows")

    return AskResponse(
        status="success",
        sql=sql,
        rows=rows,
        row_count=len(rows),
        summary="(summarization not yet implemented — Phase 4)",
    )
