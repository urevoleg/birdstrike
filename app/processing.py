
import os
import datetime as dt
import sys

import pandas as pd

from sqlalchemy import create_engine

from dotenv import load_dotenv
load_dotenv()


def upload_strike_to_db():
    df = pd.read_csv('src/strike_reports.csv', parse_dates=['INCIDENT_DATE'])
    #df['striked_at'] = pd.to_datetime(df['INCIDENT_DATE'].dt.date.astype(str) + ' ' + df['TIME'], errors='coerce')
    #df[['h', 'm']] = df['TIME'].str.split(':', expand=True)

    return df


def processing_isd() -> pd.DataFrame:
    df = pd.read_csv('src/isd-history.csv', sep=',',
                     dtype={'USAF': str, 'WBAN': str},
                     parse_dates=['BEGIN', 'END'])\
        .dropna(subset=['LAT', 'LON'])

    started_at = df['END'] >= dt.datetime(2018, 1, 1)
    df = df.loc[started_at].copy()
    df['meteostation_id'] = df['USAF'] + df['WBAN']
    return df


def create_weather_data_links(meteostation_id_list: [], year: int) -> None:
    with open('src/weather_data_links.txt', 'w') as f:
        BASE_URL = f"https://www.ncei.noaa.gov/data/global-hourly/access/{year}/"
        for mid in meteostation_id_list:
            link = BASE_URL + f"{mid}.csv\n"
            f.write(link)


def generate_link(year: int = 2018):
    df = processing_isd()

    create_weather_data_links(df['meteostation_id'].unique(), year=year)


def create_schema_for_weather_data(engine):
    from pandas.io.sql import get_schema

    df = pd.read_csv('data/2023-A0003093795.csv',
                     dtype={'STATION_ID': str},
                     sep=',', parse_dates=['DATE'])
    stmt = get_schema(df, 'weather_noaa', con=engine, schema='raw')

    with engine.connect() as con:
        con.execute(stmt)


if __name__ == '__main__':
    mode, year = sys.argv[1:]

    if mode == 'strike_isd':
        engine = create_engine(os.getenv('SQLALCHEMY_DATABASE_URI'))
        # #
        df = processing_isd()
        df.to_sql(name='isd', con=engine, if_exists='replace', schema='raw')

        df = upload_strike_to_db()
        df.to_sql(name='strike_reports', con=engine, if_exists='replace', schema='raw')

    elif mode == 'generate_and_load':
        year = int(year)
        generate_link(year=year)
    else:
        print("Not such operation!")