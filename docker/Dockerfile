
FROM astral/uv:bookworm-slim

RUN uvx carrot-transform@0.4.0 -v

RUN mkdir /app
WORKDIR /app

ENTRYPOINT ["uvx", "carrot-transform@0.4.0"]


##   λ docker build . -t ctra
##   λ docker run -it --rm ctra -v
##   λ docker run -it --rm --entrypoint   bash ctra


##   λ docker run -it --rm -v.:/app ctra -v
##   λ docker run -it --rm -v.:/app --entrypoint   bash ctra
