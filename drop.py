import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = psycopg2.connect(
    database = 'postgres',
    user='mieltn',
    host=''
)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()
create_query = 'DROP DATABASE timetable'

cursor.execute(create_query)
conn.close()