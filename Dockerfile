FROM python:2.7-slim

RUN apt-get update
RUN apt-get install -y build-essential python-virtualenv

RUN groupadd --gid 10001 app
RUN useradd --uid 10001 --gid 10001 --home /app --create-home app

WORKDIR /app

COPY . /app

RUN make build

USER app

RUN which make

ENTRYPOINT ["make"]
