from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    api_url: str
    debug_mode: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="",
        extra="ignore"
    )

settings = Settings.model_validate({})
