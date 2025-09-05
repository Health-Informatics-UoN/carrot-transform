
FROM astral/uv:bookworm-slim


RUN mkdir /app
RUN mkdir /app/run
WORKDIR /app
COPY pyproject.toml ./
COPY uv.lock ./
COPY README.md ./

COPY carrottransform/ ./carrottransform/

# run the syn command to pull in the dependencies during container creation
RUN uv sync
