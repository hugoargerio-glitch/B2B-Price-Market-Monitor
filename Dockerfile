# ==============================================================================
# Dockerfile — B2B Price & Market Monitor (Profile 1: Lean App / CDC Engine)
# Multi-stage Python 3.12-slim build with uv and non-root execution
# ==============================================================================
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY src/ ./src/
COPY tests/ ./tests/
COPY contracts/ ./contracts/
COPY knowledge/ ./knowledge/
COPY examples/ ./examples/

# ------------------------------------------------------------------------------
# Runtime Stage
# ------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

RUN groupadd -r monitor && useradd -r -g monitor -d /app -s /sbin/nologin monitor \
    && mkdir -p /app/data \
    && chown -R monitor:monitor /app

WORKDIR /app

COPY --from=builder --chown=monitor:monitor /app /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    B2B_DB_PATH="/app/data/b2b_monitor.db"

USER monitor

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["run", "--dry-run"]
