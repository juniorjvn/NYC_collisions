import pandas as pd
import numpy as np
from shapely.geometry import Point
from shapely.geometry.multipolygon import MultiPolygon
from shapely import wkt
import shapely
import requests
import os
import json
from shapely.geometry import shape
from CreateDBConnections import create_postgres_connection


mvc_db = 'motor_vehicle_collision'
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')


def get_modzcta_from_coordinates(latitude, longitude, messages=True):
    if not latitude or not longitude:
        if messages:
            print('Invalid inputs, must be real numbers')
        return None

    mvc_conn = create_postgres_connection(user, password, mvc_db, messages=False)
    mvc_cur = mvc_conn.cursor()

    point = Point(longitude, latitude)
    mvc_cur.execute('''select modzcta, geom from modzctas;''')
    modzctas = mvc_cur.fetchall()

    for modzcta in modzctas:
        zcta = modzcta[0]
        multipolygon = shape(json.loads(modzcta[1]))
        if point.intersects(multipolygon):
            mvc_conn.close()
            return zcta

    mvc_conn.close()
    return None


def get_borough_code_from_coordinates(latitude, longitude, messages=True):
    if not latitude or not longitude:
        if messages:
            print('Invalid inputs, must be real numbers')
        return None
    # Motor Vehicle Collision database connection
    mvc_conn = create_postgres_connection(user, password, mvc_db, messages=False)
    mvc_cur = mvc_conn.cursor()

    point = Point(longitude, latitude)
    mvc_cur.execute('''select name, geom from boroughs;''')
    boroughs = mvc_cur.fetchall()

    for borough in boroughs:
        name = borough[0]
        multipoly = shape(json.loads(borough[1]))
        if point.intersects(multipoly):
            mvc_conn.close()
            return name

    mvc_conn.close()
    return None