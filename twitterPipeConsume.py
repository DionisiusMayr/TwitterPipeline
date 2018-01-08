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

raw_data_file = open(RAW_DATA_PATH, "r")

tweets = []
for line in raw_data_file:
    tweets.append(json.loads(line))
    # print(line)

df = pd.DataFrame.from_dict(tweets)
raw_data_file.close()

conn = psycopg2.connect(host="localhost", database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
conn.close()

