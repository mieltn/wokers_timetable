import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import DEFAULT_DB, USER

conn = psycopg2.connect(
    database = DEFAULT_DB,
    user = USER
)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor = conn.cursor()
drop_query = 'DROP DATABASE timetable'

cursor.execute(drop_query)
conn.close()