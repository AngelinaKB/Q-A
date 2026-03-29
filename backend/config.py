from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    openai_api_key: str
    openai_model: str = "gpt-4o"
    openai_max_tokens: int = 1000
    openai_temperature: float = 0.0  # deterministic SQL generation

    class Config:
        env_file = ".env"


settings = Settings()
