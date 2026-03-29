from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # OpenAI
    openai_api_key: str
    openai_model: str = "gpt-4o"
    openai_max_tokens: int = 1000
    openai_temperature: float = 0.0  # deterministic SQL generation

    # Snowflake
    snowflake_account: str      # e.g. xy12345.us-east-1
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_warehouse: str
    snowflake_role: str = "OPS_QA_READONLY"
    snowflake_query_timeout: int = 10   # seconds — from design doc §11

    class Config:
        env_file = ".env"


settings = Settings()
