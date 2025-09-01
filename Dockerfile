
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

# specify the command to run in such a way that the parameters can be passed when invoking `docker run`
# ... also assume some default ones to minimise how much the used needs to type
# ... something will need to be done for v2, but, run_v2 isn't ready yet
ENTRYPOINT ["uv", "run", "python", "-m", "carrottransform.cli.command", "run", "mapstream", "--omop-ddl-file", "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql", "--omop-config-file", "@carrot/config/config.json"]
