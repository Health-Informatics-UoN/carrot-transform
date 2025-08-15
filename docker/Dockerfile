
FROM astral/uv:bookworm-slim

ARG CARROT_VERSION
ENV CARROT_VERSION=${CARROT_VERSION}

RUN uvx carrot-transform@${CARROT_VERSION} -v

RUN mkdir /app
WORKDIR /app

ENTRYPOINT ["uvx", "carrot-transform@${CARROT_VERSION}"]


##   λ docker build . -t ctra
##   λ docker run -it --rm ctra -v
##   λ docker run -it --rm --entrypoint   bash ctra


##   λ docker run -it --rm -v.:/app ctra -v
##   λ docker run -it --rm -v.:/app --entrypoint   bash ctra
