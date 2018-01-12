import json
import pandas as pd
import psycopg2
import itertools
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import DataFrameWriter
from sqlalchemy import create_engine
from secret_keys import DB_USER
from secret_keys import DB_NAME
from secret_keys import DB_PASSWORD

RAW_DATA_PATH = 'data/data.txt'
PREFIX = 'c_'

def addNewColumn(cur, table, column_name, column_type):
    # Checking for existing columns
    cur.execute(u"""SELECT column_name FROM information_schema.columns 
                WHERE table_name='{}' and column_name='{}'""".format(table, column_name))
    if not cur.rowcount:
        cur.execute(u"""ALTER TABLE IF EXISTS {0} ADD COLUMN {1} {2}""".format(table, column_name, column_type))

# https://www.postgresql.org/docs/9.5/static/datatype.html
# https://pandas.pydata.org/pandas-docs/stable/basics.html#dtypes
def dtypeToPSQLType(dtype):
    conversion_map = {
            'int64': 'int8',
            'int32': 'int4',
            'float64': 'float8',
            'float32': 'float4',
            'bool': 'bool',
            'datetime64': 'timestamp',
            'datetime64ns': 'timestamp', # TODO: how to store nano seconds in PSQL?
            'datetime64tz': 'time with time zone',
            'timedelta': 'interval',
            'category': 'varchar', # TODO: look up how to store a pandas category. varchar?
            'object': 'varchar'
            }

    return conversion_map[dtype]

def shortenName(name):
    abbreviations = {
            'background': 'bg',
            'coordinates': 'coord',
            'entities': 'entt',
            'extended': 'ext',
            'follow': 'fllw',
            'follower': 'fllwr',
            'following': 'fllwng',
            'image': 'img',
            'profile': 'pfl',
            'quoted': 'qotd',
            'retweet': 'rtw',
            'retweeted': 'rtwd',
            'status': 'st',
            'user': 'usr'
            }

    for (k, v) in abbreviations.iteritems():
        name = name.replace(k, v)

    return name


spark = SparkSession \
        .builder \
        .config("spark.jars", "/home/dionisius/Downloads/postgresql-42.1.4.jar") \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()
df = spark.read.json(RAW_DATA_PATH)
df = df.toPandas()
# Treat duplicated rows
df = df.drop_duplicates(subset='id')

conn = None
try:
    conn = psycopg2.connect(host="localhost", database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    TABLE = "tweets"

    cur.execute(u"DROP TABLE IF EXISTS tweets CASCADE")
    cur.execute(u"CREATE TABLE IF NOT EXISTS tweets ()")

    columns_list = []
    # Add columns to Database, if necessary
    for (column_name, column_type) in zip(list(df.columns.values.tolist()), df.dtypes):
        # Note: the prefix 'c_' is used to avoid conflicts with PSQL keywords, such as 'user'
        s = shortenName(PREFIX + str(column_name))
        addNewColumn(cur, TABLE, s, dtypeToPSQLType(str(column_type)))
        columns_list.append(s)

    cur.execute(u"ALTER TABLE IF EXISTS tweets ADD PRIMARY KEY (c_id)")

    df.columns = columns_list

    cur.close()
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

try:
    print "Writting to Database..."
    engine = create_engine('postgresql+psycopg2://' + DB_USER + ':' + DB_PASSWORD + '@localhost:5432/' + DB_NAME)
    with engine.connect() as e_conn, e_conn.begin():
        data = df.to_sql('tweets', e_conn, index=False, if_exists='append')
    print "Wrote to Database."
except (Exception, psycopg2.IntegrityError) as error:
    print(error)
