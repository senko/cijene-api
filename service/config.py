import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    def __init__(self):
        self.version: str = os.getenv("VERSION", "0.1.0")
        self.archive_dir: str = os.getenv("ARCHIVE_DIR", "data")
        self.base_url: str = os.getenv("BASE_URL", "https://api.cijene.dev")
        self.host: str = os.getenv("HOST", "0.0.0.0")
        self.port: int = int(os.getenv("PORT", "8000"))
        self.debug: bool = os.getenv("DEBUG", "false").lower() == "true"
        self.timezone: str = os.getenv("TIMEZONE", "Europe/Zagreb")
        self.redirect_url: str = os.getenv("REDIRECT_URL", "https://cijene.dev")


settings = Settings()
