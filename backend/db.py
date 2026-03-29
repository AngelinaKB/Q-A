import snowflake.connector
from snowflake.connector import DictCursor
from contextlib import contextmanager

from backend.config import settings


class DBError(Exception):
    pass


class QueryTimeoutError(DBError):
    pass


def _build_connect_kwargs() -> dict:
    """
    Build Snowflake connection kwargs from env config.
    Supports two auth methods:
      - "password"         — standard user/password
      - "externalbrowser"  — SSO / web-based login (Okta, Azure AD, etc.)
    """
    kwargs = dict(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        warehouse=settings.snowflake_warehouse,
        role=settings.snowflake_role,
        session_parameters={
            "STATEMENT_TIMEOUT_IN_SECONDS": str(settings.snowflake_query_timeout),
        },
    )

    method = settings.snowflake_auth_method.lower()

    if method == "externalbrowser":
        kwargs["authenticator"] = "externalbrowser"
    elif method == "password":
        if not settings.snowflake_password:
            raise DBError(
                "SNOWFLAKE_PASSWORD is required when SNOWFLAKE_AUTH_METHOD=password"
            )
        kwargs["password"] = settings.snowflake_password
    else:
        raise DBError(
            f"Unknown SNOWFLAKE_AUTH_METHOD={method!r}. "
            "Valid values: 'password', 'externalbrowser'"
        )

    return kwargs


def _get_connection():
    try:
        return snowflake.connector.connect(**_build_connect_kwargs())
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
    Row cap is read from settings.max_rows — never hardcoded.
    """
    with get_connection() as conn:
        try:
            cursor = conn.cursor(DictCursor)
            cursor.execute(sql)
            rows = cursor.fetchmany(settings.max_rows)
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
