FROM python:3.10-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /uvx /bin/

# Git is required for hatch-vcs to resolve the package version from git tags
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-editable --no-dev

# Copy the project into the intermediate image
COPY . /app

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --no-dev

# ------
FROM python:3.10-slim

# Create a non-root user and group
RUN groupadd -r app && useradd --no-log-init -r -g app app

LABEL org.opencontainers.image.title="Carrot Transform"
LABEL org.opencontainers.image.description="Carrot Transform"
LABEL org.opencontainers.image.vendor="University of Nottingham"
LABEL org.opencontainers.image.url=https://github.com/Health-Informatics-UoN/carrot-transform/pkgs/container/carrot%2Ftransform
LABEL org.opencontainers.image.source=https://github.com/Health-Informatics-UoN/carrot-transform
LABEL org.opencontainers.image.licenses=MIT

# Copy the environment, but not the source code
COPY --from=builder --chown=app:app /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

# Switch to the non-root user
USER app

CMD ["carrot-transform"]
