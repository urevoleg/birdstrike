import os
import datetime as dt
import sys

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


if __name__ == '__main__':
    filepath = sys.argv[1]
    load_from_file_to_db(filepath=filepath)

    """
    curl https://www.ncei.noaa.gov/access/services/data/v1\?startDate\=2018-01-02T05:00:00\&endDate\=2018-01-03T00:00:00\&dataset\=global-hourly\&stations\=72327013897\&format\=json
    """

    """
    create function, which requests url https://www.ncei.noaa.gov/access/services/data/v1\?startDate\=2018-01-02T05:00:00\&endDate\=2018-01-03T00:00:00\&dataset\=global-hourly\&stations\=72327013897\&format\=json
    save response to json variable and create dataset from response
    """
    # def fetch_data_from_noaa(baseurl: str, params: dict = {})-> pd.DataFrame:
    #     response = requests.get(url=baseurl, params=params)
    #     response.raise_for_status()
    #     return pd.DataFrame(response.json())
    #
    #
    # params = {
    #     'startDate': '2018-01-02T05:00:00',
    #     'endDate': '2018-01-03T00:00:00',
    #     'dataset': 'global-hourly',
    #     'stations': '72327013897',
    #     'format': 'json'
    # }
    # df = fetch_data_from_noaa(baseurl = "https://www.ncei.noaa.gov/access/services/data/v1", params=params)
    # print(df.head())