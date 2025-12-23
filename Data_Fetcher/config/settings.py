from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    ENV: str = "dev"
    DEFAULT_TIMEOUT_SEC: int = 30
    DEFAULT_RETRIES: int = 2
    TTL_SECONDS: int = 600  # in-memory TTL for reference maps

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
