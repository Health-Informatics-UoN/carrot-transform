
FROM astral/uv:bookworm-slim


RUN mkdir /app
RUN mkdir /app/run
WORKDIR /app
COPY pyproject.toml ./
COPY uv.lock ./
COPY README.md ./

COPY carrottransform/ ./carrottransform/

# run help to pull stuff in and be sure it's all working
RUN uv run python -m carrottransform.cli.command run mapstream --help
