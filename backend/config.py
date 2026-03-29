from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # OpenAI
    openai_api_key: str
    openai_model: str
    openai_max_tokens: int
    openai_temperature: float

    # Snowflake connection
    snowflake_account: str
    snowflake_user: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_warehouse: str
    snowflake_role: str
    snowflake_query_timeout: int

    # Snowflake auth method: "password" or "externalbrowser" (SSO/web)
    snowflake_auth_method: str

    # Only required when snowflake_auth_method=password
    snowflake_password: str = ""

    # App
    max_rows: int
    cors_allow_origin: str

    class Config:
        env_file = ".env"


settings = Settings()
