"""
Configuration and SQLite PRAGMAs for Profile 1 (Lean App/CDC).
Target: SQLite embedded database running in WAL mode.
"""

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path("/home/Hugo/b2b-price-monitor").resolve()
DEFAULT_DB_PATH = PROJECT_ROOT / "b2b_monitor.db"


@dataclass(frozen=True)
class DatabaseConfig:
    db_path: Path = DEFAULT_DB_PATH
    busy_timeout_ms: int = 5000
    journal_mode: str = "WAL"
    synchronous: str = "NORMAL"
    foreign_keys: bool = True

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
