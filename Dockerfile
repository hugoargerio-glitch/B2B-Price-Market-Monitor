# ==============================================================================
# Dockerfile — B2B Price & Market Monitor (Profile 1: Lean App / CDC Engine)
# Multi-stage Python 3.12-slim build with uv and non-root execution (UID 10001)
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Builder (Lean virtualenv with production/runtime dependencies only)
# ------------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# ------------------------------------------------------------------------------
# Stage 2: Test Target Stage (includes dev dependencies, tests, contracts, docs)
# ------------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS test

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

COPY src/ ./src/
COPY tests/ ./tests/
COPY contracts/ ./contracts/
COPY knowledge/ ./knowledge/
COPY examples/ ./examples/
COPY README.md LICENSE ./

RUN groupadd -g 10001 monitor \
    && useradd -u 10001 -g 10001 -d /app -s /sbin/nologin monitor \
    && mkdir -p /app/data \
    && chown -R 10001:10001 /app

USER monitor

ENTRYPOINT ["pytest"]
CMD ["-v"]

# ------------------------------------------------------------------------------
# Stage 3: Runtime Stage (Lean Production Image without test/dev dependencies)
# ------------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

RUN groupadd -g 10001 monitor \
    && useradd -u 10001 -g 10001 -d /app -s /sbin/nologin monitor \
    && mkdir -p /app/data \
    && chown -R 10001:10001 /app

WORKDIR /app

COPY --from=builder --chown=10001:10001 /app/.venv /app/.venv
COPY --chown=10001:10001 src/ ./src/
COPY --chown=10001:10001 examples/ ./examples/
COPY --chown=10001:10001 README.md LICENSE ./

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    B2B_DB_PATH="/app/data/b2b_monitor.db"

USER monitor

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["run", "--dry-run"]
