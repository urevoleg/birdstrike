import os
import datetime as dt
import sys

from dateutil.parser import parse

import itertools

import requests

import pandas as pd

from sqlalchemy import create_engine, text

from dotenv import load_dotenv
load_dotenv()


def load_from_file_to_db(filepath:str):
    engine = create_engine(os.getenv('SQLALCHEMY_DATABASE_URI'))

    columns = ['STATION', 'DATE', 'SOURCE', 'LATITUDE', 'LONGITUDE',
               'ELEVATION', 'NAME', 'REPORT_TYPE',
               'CALL_SIGN', 'QUALITY_CONTROL', 'WND',
               'CIG', 'VIS', 'TMP', 'DEW', 'SLP']

    with engine.connect() as con:
        df = pd.read_csv(filepath, parse_dates=['DATE'],
                         dtype={'STATION': str})[columns]

        df.to_sql(name='weather_noaa', con=con, schema='raw', if_exists='append', index=None)

        stmt = text("""INSERT INTO raw.service (filename) VALUES(:filepath)""")
        con.execute(stmt,
                    filepath=filepath)


def retries(func):
    def wrapper(*args, **kwargs):
        for i in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                pass
        return {}
    return wrapper


@retries
def fetch_data_from_noaa(started_at: dt.datetime, ended_at: dt.datetime,
                         meteostation_id: str, **kwargs)-> pd.DataFrame:
    baseurl = "https://www.ncei.noaa.gov/access/services/data/v1"

    params = {
        'startDate': started_at,
        'endDate': ended_at,
        'dataset': 'global-hourly',
        'stations': meteostation_id,
        'format': 'json'
    }

    response = requests.get(url=baseurl, params=params)
    response.raise_for_status()
    return response.json()


def chunker(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk


if __name__ == '__main__':
    # filepath = sys.argv[1]
    # load_from_file_to_db(filepath=filepath)

    """
    curl https://www.ncei.noaa.gov/access/services/data/v1\?startDate\=2018-01-02T05:00:00\&endDate\=2018-01-03T00:00:00\&dataset\=global-hourly\&stations\=72327013897\&format\=json
    """
    def gen_incident_chunk():
        engine = create_engine(os.getenv('SQLALCHEMY_DATABASE_URI'))

        with engine.connect() as con:
            with open('sql/raw/meteostation_with_incidented_at.sql', 'r') as f:
                stmt = text(f.read())
            rows = con.execute(stmt,
                               dated_at=dt.datetime(2018, 1, 1, 0, 0, 0),)
            for row in rows:
                yield row


    for chunk_rows in chunker(gen_incident_chunk(), 10):
        output = []

        def row_handler(row):
            payload = {
                **row._asdict(),
            }
            payload['started_at'] = payload['started_at'].isoformat()
            payload['ended_at'] = payload['ended_at'].isoformat()
            return payload

        for row in chunk_rows:
            #TODO надо обработать ошибки от API
            response_data = fetch_data_from_noaa(**row_handler(row))
            if not response_data == []:
                near_row = sorted(response_data, key=lambda x: parse(x['DATE']) - row.raw_incidented_at)[0]
            else:
                near_row = {}
            output += [{**row._asdict(), **near_row}]
        print(output)
        break

