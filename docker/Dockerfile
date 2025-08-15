
FROM astral/uv:bookworm-slim

ARG CARROT_VERSION
ENV CARROT_VERSION=${CARROT_VERSION}

RUN uvx carrot-transform@${CARROT_VERSION} -v

RUN mkdir /app
WORKDIR /app

ENTRYPOINT ["uvx", "carrot-transform@${CARROT_VERSION}"]

