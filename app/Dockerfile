FROM ubuntu:20.04

RUN mkdir -p /app

RUN apt-get update && apt-get install -y tzdata
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt install --yes --force-yes mdbtools python3 python3-pip wget unzip lsb-release

RUN sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt update
RUN apt install libpq-dev postgresql-client-13

RUN echo "db:5432:birdstrike:pgadmin:12345" > ~/.pgpass && chmod 0600 ~/.pgpass

WORKDIR /app

COPY ./app/requirements.txt .

RUN python3 -m pip install -r requirements.txt

COPY ./app/ /app

CMD ["python3", "app.py"]