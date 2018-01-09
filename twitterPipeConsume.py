import json
import pandas as pd
import psycopg2

from secret_keys import DB_USER
from secret_keys import DB_NAME
from secret_keys import DB_PASSWORD

RAW_DATA_PATH = 'data/data.txt'

def print_json_obj(obj):
    print_json_obj_aux(obj, True)

def print_json_keys(obj):
    print_json_obj_aux(obj, False)

def print_json_obj_aux(obj, complete_print, indent=0):
    if isinstance(obj, dict):
        for key, value in obj.iteritems():
            print ' ' * indent + str(key)
            if complete_print:
                print ':'
            print_json_obj_aux(value, complete_print, indent + 2)
    elif isinstance(obj, list):
        for v in obj:
            print_json_obj_aux(v, complete_print, indent + 2)
    else:
        if complete_print:
            print ' ' * indent, obj # key, 'is: ', value

def addNewColumn(cur, table, column_name, column_type):
    cur.execute("""ALTER TABLE IF EXISTS {0} ADD COLUMN {1} {2};""".format(table, column_name, column_type))

# https://www.postgresql.org/docs/9.5/static/datatype.html
# https://pandas.pydata.org/pandas-docs/stable/basics.html#dtypes
def dtypeToPSQLType(dtype):
    conversion_map = {
            'int64':'int8',
            'int32':'int4',
            'float64':'float8',
            'float32':'float4',
            'bool':'bool',
            'datetime64':'timestamp',
            'datetime64ns':'timestamp', # TODO: how to store nano seconds in PSQL?
            'datetime64tz':'time with time zone',
            'timedelta':'interval',
            'category':'varchar', # TODO: look up how to store a pandas category. varchar?
            'object':'varchar'
            }

    return conversion_map[dtype]


raw_data_file = open(RAW_DATA_PATH, "r")

tweets = []
with open(RAW_DATA_PATH, "r") as raw_data_file:
    for line in raw_data_file:
        tweets.append(json.loads(line))
raw_data_file.close()

df = pd.DataFrame.from_dict(tweets)

# print df['entities'].dtypes

conn = None
try:
    conn = psycopg2.connect(host="localhost", database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()
    TABLE = "tweets"

    cur.execute("DROP TABLE IF EXISTS tweets CASCADE")
    cur.execute("CREATE TABLE IF NOT EXISTS tweets (id integer PRIMARY KEY);")

    # Add columns to Database, if necessary
    for (column_name, column_type) in zip(list(df.columns.values.tolist()), df.dtypes):
        # print "{}\t---> {}".format(column_type, dtypeToPSQLType(str(column_type)))
        # Note: the prefix 'c_' is used to avoid conflicts with PSQL keywords, such as 'user'
        addNewColumn(cur, TABLE, 'c_' + str(column_name), dtypeToPSQLType(str(column_type)))

    cur.close()
    conn.commit()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
