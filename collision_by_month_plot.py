import os
import json
from datetime import datetime
from CreateDBConnections import create_postgres_connection


mvc_db = 'motor_vehicle_collision'
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASS')

conn = create_postgres_connection(user, password, mvc_db)
cur = conn.cursor()

stm = '''
SELECT borough_id, month, AVG(accidents)
FROM
    (SELECT borough_id, EXTRACT(year from crash_date) as year,
            EXTRACT(month FROM crash_date) as month, COUNT(*) as accidents
    FROM collisions
    WHERE borough_id IS NOT NULL
    GROUP BY borough_id, year, month) as accidents_per_year_month
GROUP BY borough_id, month
ORDER BY borough_id, month;
'''
cur.execute(stm)
rows = cur.fetchall()

collision_by_month = {}
for row in rows:
    collisions = collision_by_month.get(int(row[1]), [])
    collision_by_month[int(row[1])] = collisions + [int(row[2])]

boroughs = {"MANHATTAN": 1, "BRONX": 2, "BROOKLYN": 3, "QUEENS": 4, "STATEN ISLAND": 5}

f = open('monthlyCollisions.js', 'w')
f.write('monthlyCollisions = [["Month", ' + json.dumps(list(boroughs.keys()))[1:])
for month in collision_by_month:
    month_name = datetime.strptime(str(month), '%m').strftime('%B')
    f.write(',\n["' + month_name + '", ' + ', '.join(map(str, collision_by_month[month])) + ']')
f.write('\n];\n')
f.close()

conn.close()


