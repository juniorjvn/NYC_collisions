import os
import requests
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from CreateDBConnections import create_postgres_connection
from SearchByCoordinates import get_modzcta_from_coordinates
from SearchByCoordinates import get_borough_code_from_coordinates

from shapely.geometry import shape
from shapely.geometry import Point
from shapely import wkt

nyc_od_db = 'nyc_open_data'
mvc_db = 'motor_vehicle_collision'
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')
table_name = 'collisions'

# NYC Open Data Connection, read only mode
nyc_od_conn = create_postgres_connection(user,password, nyc_od_db)
nyc_od_conn.set_session(readonly=True)
nyc_od_cur = nyc_od_conn.cursor(cursor_factory=RealDictCursor)

# Motor Vehicle Collision database connection
mvc_conn = create_postgres_connection(user, password, mvc_db)
mvc_cur = mvc_conn.cursor()

create_table_collisions_stm = '''
CREATE TABLE IF NOT EXISTS collisions(
    collision_id SERIAL,
    borough_id VARCHAR(15),
    crash_date DATE,
    crash_time TIME,
    modzcta VARCHAR(5),
    PRIMARY KEY(collision_id)
);
'''
mvc_cur.execute(create_table_collisions_stm)
mvc_conn.commit()

num_of_records = int(input('how many records: '))
nyc_od_cur.execute('''SELECT MIN(crash_date) FROM collisions;''')
crash_date = nyc_od_cur.fetchall()[0]['min']
crash_time = '00:00:00'

mvc_cur.execute('''SELECT crash_date, crash_time FROM collisions ORDER BY crash_date DESC, crash_time DESC LIMIT 1;''')
latest_date = mvc_cur.fetchone()
# if MVC database is not empty, date and time will be updated
if latest_date:
    crash_date = latest_date[0]
    crash_time = latest_date[1]
    num_of_records += 1


select_collisions_tsm = '''
SELECT collision_id, crash_date, crash_time, borough, zip_code, latitude, longitude
FROM collisions
WHERE crash_date >= %s and crash_time >= %s
ORDER BY crash_date, crash_time
LIMIT %s;'''

print("Retrieving data...")
nyc_od_cur.execute(select_collisions_tsm, (crash_date, crash_time, num_of_records))
nyc_od_collisions = nyc_od_cur.fetchall()

boroughs = {"MANHATTAN": 1, "BRONX": 2, "BROOKLYN": 3, "QUEENS": 4, "STATEN ISLAND": 5}
insert_stm = '''INSERT INTO collisions(collision_id, borough_id, crash_date, crash_time, modzcta)
                            VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING collision_id;'''
counter = 0
if nyc_od_collisions:
    print("Inserting new records into collisions table...")
for collision in nyc_od_collisions:
    # Insert values with a valid date only
    # if collision date is not None
    id = collision.get('collision_id')
    mvc_cur.execute('''SELECT collision_id FROM collisions WHERE collision_id = %s''', (id,))
    found = mvc_cur.fetchone()
    if found:
        continue

    borough = collision.get('borough')
    borough_id = None
    crash_date = collision.get('crash_date')
    crash_time = collision.get('crash_time')
    zip_code = collision.get('zip_code')
    modzcta = '88888'

    # Won't insert records with no date
    if crash_date is not None:
        # Try to get modzcta is zip code is None but have valid latitude and longitude
        if zip_code is None and collision['latitude'] and collision['longitude']:
            # If a valid modzcta can be found , it will bi assigned to 'modzcta' variable
            temp_modzcta = get_modzcta_from_coordinates(collision['latitude'], collision['longitude'])
            if temp_modzcta:
                modzcta = temp_modzcta
        # If zip_code is None, will keep the record with 88888 as a zip code to indicate
        # that the accident occurred somewhere in NYC
        if zip_code is not None:
            mvc_cur.execute('''SELECT modzcta FROM zipcodes WHERE zipcode = %s;''', (zip_code, ))
            temp_modzcta = mvc_cur.fetchall()
            if temp_modzcta:
                modzcta = temp_modzcta[0][0]
        # Try to get borough_id if record has a valid latitude and longitude
        if borough is None and collision['latitude'] and collision['longitude']:
            borough = get_borough_code_from_coordinates(collision['latitude'], collision['longitude'])
        if borough:
            borough_id = boroughs.get(borough.upper())

        try:
            mvc_cur.execute(insert_stm, (id, borough_id, crash_date, crash_time, modzcta))
            valid_id = mvc_cur.fetchone()
            if valid_id:
                counter += 1
            mvc_conn.commit()

        except psycopg2.Error as e:
            mvc_conn.rollback()
            mvc_conn.close()
            print('Error inserting values {} into Postgresql DB.'.format(collision))
            print('Error:', e)
            exit()
# mvc_conn.commit()
nyc_od_conn.close()
mvc_conn.close()

if counter == 0:
    print('\nThe Database is up to date.')
else:
    print('\nNew collision records were successfully inserted')
