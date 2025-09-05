
FROM astral/uv:bookworm-slim


# RUN mkdir /app
WORKDIR /app
COPY README.md pyproject.toml uv.lock ./
COPY carrottransform/ ./carrottransform/



# run the sync command to pull in the dependencies during container creation
RUN uv sync --frozen
