import os
import datetime as dt

from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_api import status

from dotenv import load_dotenv
load_dotenv()


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
db = SQLAlchemy(app=app)


@app.route('/')
def index():
    return jsonify({
        'dt': dt.datetime.now(),
        'db': {
            'status': db.engine.connect().execute("""SELECT EXTRACT(EPOCH from max(loaded_at) - min(loaded_at))/60 as load_duration FROM raw.service""").fetchone()._asdict(),
        }
    }, status.HTTP_200_OK)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)