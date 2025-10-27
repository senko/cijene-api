import os
from dotenv import load_dotenv

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from service.db.base import Database

load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""

    _db: "Database | None" = None

    def __init__(self):
        self.version: str = os.getenv("VERSION", "0.1.0")
        self.archive_dir: str = os.getenv("ARCHIVE_DIR", "data")
        self.root_path: str = os.getenv("ROOT_PATH", "/")
        self.base_url: str = os.getenv("BASE_URL", "https://api.cijene.dev")
        self.host: str = os.getenv("HOST", "0.0.0.0")
        self.port: int = int(os.getenv("PORT", "8000"))
        self.debug: bool = os.getenv("DEBUG", "false").lower() == "true"
        self.timezone: str = os.getenv("TIMEZONE", "Europe/Zagreb")
        self.redirect_url: str = os.getenv("REDIRECT_URL", "https://cijene.dev")

        # Database configuration
        self.db_dsn: str = os.getenv(
            "DB_DSN",
            "postgresql://postgres:postgres@localhost/cijene",
        )
        self.db_min_connections: int = int(os.getenv("DB_MIN_CONNECTIONS", "5"))
        self.db_max_connections: int = int(os.getenv("DB_MAX_CONNECTIONS", "20"))

    def get_db(self) -> "Database":
        """
        Get the database instance based on the configured settings.

        This method initializes the singleton database connection
        if it hasn't been done yet, and returns the instance.

        Returns:
            An instance of the Database subclass based on the DSN.

        """
        from service.db.base import Database

        if self._db is None:
            self._db = Database.from_url(
                self.db_dsn,
                min_size=self.db_min_connections,
                max_size=self.db_max_connections,
            )

        return self._db


settings = Settings()
