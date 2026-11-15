"""
Configuration and SQLite PRAGMAs for Profile 1 (Lean App/CDC).
Target: SQLite embedded database running in WAL mode.
"""

from dataclasses import dataclass, field
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_default_db_path() -> Path:
    """Return default SQLite database path from B2B_DB_PATH or fallback to repo root."""
    env_path = os.getenv("B2B_DB_PATH")
    if env_path and env_path.strip():
        return Path(env_path.strip()).resolve()
    return (PROJECT_ROOT / "b2b_monitor.db").resolve()


DEFAULT_DB_PATH = get_default_db_path()


@dataclass(frozen=True)
class DatabaseConfig:
    db_path: Path = field(default_factory=get_default_db_path)
    busy_timeout_ms: int = 5000
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    foreign_keys: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.db_path, str):
            object.__setattr__(self, "db_path", Path(self.db_path).resolve())

    @property
    def pragma_statements(self) -> list[str]:
        return [
            f"PRAGMA journal_mode = {self.journal_mode};",
            f"PRAGMA synchronous = {self.synchronous};",
            f"PRAGMA busy_timeout = {self.busy_timeout_ms};",
            f"PRAGMA foreign_keys = {'ON' if self.foreign_keys else 'OFF'};",
        ]


@dataclass(frozen=True)
class CircuitBreakerConfig:
    max_quarantine_rate: float = 0.20  # 20% template drift threshold
    min_sample_size: int = 10  # Minimum domain sample floor before tripping percentage breaker
    max_retries: int = 5
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 300.0
