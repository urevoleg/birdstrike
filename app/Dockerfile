FROM python:3.10-alpine

RUN mkdir -p /app
WORKDIR /app

COPY ./app/requirements.txt .

RUN python3 -m pip install -r requirements.txt

COPY ./app/ /app

CMD ["python", "app.py"]