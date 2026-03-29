import snowflake.connector
from snowflake.connector import DictCursor
from contextlib import contextmanager

from backend.config import settings

MAX_ROWS = 1000  # hard cap — matches design doc §11


class DBError(Exception):
    pass


class QueryTimeoutError(DBError):
    pass


def _get_connection():
    """
    Open a fresh Snowflake connection.
    We open per-request in Phase 2; a connection pool can be layered
    in during Phase 7 production hardening.
    """
    try:
        conn = snowflake.connector.connect(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            password=settings.snowflake_password,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
            warehouse=settings.snowflake_warehouse,
            role=settings.snowflake_role,
            # Enforce query timeout at the session level
            session_parameters={
                "STATEMENT_TIMEOUT_IN_SECONDS": str(settings.snowflake_query_timeout),
            },
        )
        return conn
    except snowflake.connector.errors.DatabaseError as e:
        raise DBError(f"Could not connect to Snowflake: {e}") from e


@contextmanager
def get_connection():
    conn = _get_connection()
    try:
        yield conn
    finally:
        conn.close()


def execute_query(sql: str) -> list[dict]:
    """
    Execute a validated SELECT query and return rows as a list of dicts.
    Enforces MAX_ROWS even if the SQL somehow slipped through without a LIMIT.
    Raises DBError on failure, QueryTimeoutError on timeout.
    """
    with get_connection() as conn:
        try:
            cursor = conn.cursor(DictCursor)
            cursor.execute(sql)
            rows = cursor.fetchmany(MAX_ROWS)

            # Convert to plain dicts (some Snowflake types need coercion)
            return [_coerce_row(r) for r in rows]

        except snowflake.connector.errors.ProgrammingError as e:
            msg = str(e)
            if "timeout" in msg.lower() or "statement timeout" in msg.lower():
                raise QueryTimeoutError(
                    f"Query exceeded {settings.snowflake_query_timeout}s timeout"
                ) from e
            raise DBError(f"Query execution failed: {e}") from e
        except snowflake.connector.errors.DatabaseError as e:
            raise DBError(f"Snowflake error: {e}") from e


def _coerce_row(row: dict) -> dict:
    """
    Snowflake returns some types (Decimal, date, datetime) that aren't
    JSON-serialisable by default. Convert them to standard Python types.
    """
    from decimal import Decimal
    from datetime import date, datetime

    result = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            result[k] = float(v)
        elif isinstance(v, (datetime, date)):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result
