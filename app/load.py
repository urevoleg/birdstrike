import os
import datetime as dt
import sys

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