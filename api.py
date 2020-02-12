from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import pickle as p
import pandas as pd
import json
import datetime as dt
from datetime import datetime
import pymysql
from flask_cors import CORS
import urllib3
import xmltodict

app = Flask(__name__)
CORS(app)


def getCurrentState(id):
    url = "http://www.valenbisi.es/service/stationdetails/valence/" + str(id)
    http = urllib3.PoolManager()
    response = http.request('GET', url)
    try:
        data = xmltodict.parse(response.data)
    except:
        data = {}
    return data


def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
            + dt.timedelta(hours=t.minute//30))


@app.route('/api/prediction', methods=['GET'])
def apiPrediction():
    station = int(request.args.get('station'))
    hoursCounter = int(request.args.get('hours'))
    date = datetime.today()

    if request.args.get('date'):
        date = dt.datetime.strptime(
            request.args.get('date'), '%Y-%m-%dT%H:%M:%S')

    currentState = getCurrentState(station)
    currentAvailable = currentState['station']['available']
    currentTotal = currentState['station']['total']

    conn = pymysql.connect(
        host='34.69.136.137',
        port=int(3306),
        user='root',
        passwd='rtfgvb77884',
        db='valenbisi',
        charset='utf8mb4')

    df_weather_snapshot = pd.read_sql_query(
        "SELECT temperature, humidity, wind_speed, cloud_percentage, creation_date FROM weather order by creation_date desc limit 1", conn)
    df_weather = df_weather_snapshot.rename(index=str, columns={
                                            "wind_speed": "wind", "cloud_percentage": "cloud", "creation_date": "datetime"})
    df_event_snapshot = pd.read_sql_query(
        "SELECT  football, basketball FROM sport_event where date = date('%s-%s-%s') limit 1" % (date.year, date.month, date.day), conn)
    df_holiday_snapshot = pd.read_sql_query(
        "SELECT enabled holiday, enabled FROM holiday where date = date('%s-%s-%s') limit 1" % (date.year, date.month, date.day), conn)

    conn.close()

    available = currentAvailable
    iterDate = hour_rounder(date)

    for i in range(hoursCounter):
        iterDate = (iterDate + dt.timedelta(hours=(i + 1)))

        model_input = [[iterDate.year,  # year
                        iterDate.month,  # month
                        iterDate.weekday(),  # dayofweek
                        iterDate.hour,  # hour
                        df_holiday_snapshot['holiday'],  # holiday
                        df_event_snapshot['football'],  # football
                        df_event_snapshot['basketball'],  # basketball
                        df_weather['temperature'],  # temperature
                        df_weather['humidity'],  # humidity
                        df_weather['wind'],  # wind
                        df_weather['cloud'],  # cloud
                        available,  # available
                        ]]

        try:
            model = p.load(open('models/model_{}.pkl'.format(station), 'rb'))
        except:
            model = p.load(open('models/model_1.pkl', 'rb'))

        result = model.predict(model_input)

        available = round(result[0])

    if int(available) >= int(currentTotal):
        available = currentTotal

    return jsonify(
        station=station,
        date=(date + dt.timedelta(hours=hoursCounter)),
        available=available,
        total=currentTotal,
        perc=(available/int(currentTotal))
    )


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
