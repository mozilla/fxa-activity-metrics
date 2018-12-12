FROM python:2.7-slim

RUN \
    groupadd --gid 10001 app && \
    useradd --uid 10001 --gid 10001 --home /app --create-home app

WORKDIR /app

COPY . /app

make build

USER app

RUN which make

ENTRYPOINT ["make"]
