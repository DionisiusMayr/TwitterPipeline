"""
Dosctring
"""
import os
import json
import re
import pandas as pd
import psycopg2

from sqlalchemy import create_engine
from ..config import DB_CONFIG
from ..config import DATA_PATH

TABLE = "tweets"
BAD_TABLE = "bad_tweets"
PREFIX = 'c_'


def get_last_json(cur, table):
    """Docstring"""
    cur.execute(u"""CREATE TABLE IF NOT EXISTS meta_data (last_json VARCHAR,
            last_dir VARCHAR)""")
    cur.execute(u"""SELECT last_json, last_dir FROM {}""".format(table))
    if cur.rowcount:
        return cur.fetchone()
    else:
        return ("0", "0")


def add_new_column(cur, table, col_name, column_type):
    """Docstring"""
    # Checking for existing columns
    # print " Adding new column", column_name, "type:", column_type
    cur.execute(u"""SELECT column_name FROM information_schema.columns WHERE
            table_name='{}' and column_name='{}'""".format(table, col_name))
    if not cur.rowcount:
        print "Adding new column {}...".format(col_name)
        cur.execute(u"""ALTER TABLE IF EXISTS {0} ADD COLUMN {1}
                {2}""".format(table, col_name, column_type))
    # print " Finished adding new column", col_name, ".\n"


# https://www.postgresql.org/docs/9.5/static/datatype.html
# https://pandas.pydata.org/pandas-docs/stable/basics.html#dtypes
def dtype_to_psql_type(dtype):
    """Docstring"""
    conversion_map = {
        'int64': 'int8',
        'int32': 'int4',
        'float64': 'float8',
        'float32': 'float4',
        'bool': 'bool',
        'datetime64': 'timestamp',
        'datetime64ns': 'timestamp',  # TODO: how to store ns in PSQL?
        'datetime64tz': 'time with time zone',
        'timedelta': 'interval',
        'category': 'varchar',  # TODO: how to store a pandas category.
        'object': 'varchar'
        }

    return conversion_map[dtype]


def shorten_name(name):
    """Docstring"""
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

    for (key, value) in abbreviations.iteritems():
        name = name.replace(key, value)

    return name


def update_metadata(cur, last_json, last_dir):
    """Docstring"""
    cur.execute(u"""UPDATE meta_data SET last_json = {0},
            last_dir = {1}""".format(last_json, last_dir))
    if not cur.rowcount:
        cur.execute(u"""INSERT INTO meta_data (last_json, last_dir)
                VALUES ('{0}', '{1}')""".format(last_json, last_dir))


def connect_db(host, database, user, password):
    """Docstring"""
    conn = psycopg2.connect(host=host, database=database,
                            user=user,
                            password=password)
    cur = conn.cursor()
    return (conn, cur)


def add_json_to_db(json_path, engine):
    """Docstring"""
    conn, cur = None, None
    try:
        (conn, cur) = connect_db("localhost", DB_CONFIG['name'],
                                 DB_CONFIG['user'],
                                 DB_CONFIG['password'])

        tweets = []
        with open(json_path, 'r') as jfile:
            for line in jfile:
                tweets.append(json.loads(line))
        data_frame = pd.DataFrame.from_dict(tweets)

        for row in data_frame.itertuples(index=True, name=None):
            index = row[0]
            for (row_data, col) in zip(row[1:],
                                       data_frame.columns.values.tolist()):
                if isinstance(row_data,
                              (int, long, float, bool, unicode, str)) \
                        or row_data is None:
                    # TODO REMOVE THIS LINE
                    # data_frame.at[index, col] = unicode(row_data)
                    pass
                elif isinstance(row_data, (dict, list)):
                    data_frame.at[index, col] = unicode(row_data)
                else:
                    print "Warning: untreated type:", type(row_data), row_data

        # data_frame = data_frame.infer_objects()
        # Add columns to Database, if necessary
        columns_list = []
        for (col_name, column_type) in zip(
                list(data_frame.columns.values.tolist()),
                data_frame.dtypes):
            # Note: the prefix 'c_' is used to avoid conflicts with PSQL
            # keywords, such as 'user'
            short_cname = PREFIX + shorten_name(str(col_name))
            columns_list.append(short_cname)
        data_frame.columns = columns_list

        cur.execute(u"""SELECT table_name FROM information_schema.tables """
                    u"""WHERE table_name='{}'""".format(TABLE))
        if not cur.rowcount:  # Table inexistent
            print "Creating table {}...".format(TABLE)
            schema_query = pd.io.sql.get_schema(frame=data_frame, name=TABLE,
                                                con=conn, keys='c_id')
            print data_frame
            print "DTYPES"
            print data_frame.dtypes
            print "SCHEMA QUERY"
            print schema_query
            cur.execute(schema_query)
            conn.commit()
            print "Table {} created.\n".format(TABLE)
            for (col_name, column_type) in zip(
                    list(data_frame.columns.values.tolist()),
                    data_frame.dtypes):
                # TODO: Remove this (converts every column to varchar)
                cur.execute(u"""ALTER TABLE {0} ALTER COLUMN {1}
                        TYPE {2}""".format(TABLE, col_name, "varchar"))
                conn.commit()
        else:
            for (col_name, column_type) in zip(
                    list(data_frame.columns.values.tolist()),
                    data_frame.dtypes):
                add_new_column(cur, TABLE, col_name,
                               dtype_to_psql_type(str(column_type)))
                cur.execute(u"""ALTER TABLE {0} ALTER COLUMN {1}
                        TYPE {2}""".format(TABLE, col_name, "varchar"))
                conn.commit()

        try:
            with engine.connect() as e_conn, e_conn.begin():
                data_frame.to_sql(TABLE, e_conn, index=False,
                                  if_exists='append')
                # e_conn.close()
        except (Exception, psycopg2.IntegrityError) as error:
            with open(json_path, 'r') as json_file:
                data = json_file.read()
                pattern = re.compile(r".*?\"timestamp_ms\":\"(\d+)\",?.*?")
                tweet_ts_ms = pattern.match(data).group(1)
                error_msg = str(error).split('\n')[0]
                print "  Bad data: ts_ms:", tweet_ts_ms, "Error:", error_msg

                cur.execute(u"""SELECT table_name """
                            u"""FROM information_schema.tables """
                            u"""WHERE table_name='{0}'""".format(BAD_TABLE))
                if not cur.rowcount:  # Table inexistent
                    print "Creating table {0}...".format(BAD_TABLE)
                    cur.execute(u"""CREATE TABLE IF NOT EXISTS {0} """
                                u"""(ts_ms VARCHAR, raw_data VARCHAR, """
                                u"""error_msg VARCHAR)""".format(BAD_TABLE))
                    conn.commit()
                    print "Table {0} created.\n".format(BAD_TABLE)

                print
                cur.execute(u"""INSERT INTO {0} (ts_ms, raw_data, error_msg)"""
                            u"""VALUES (%s, %s, %s)""".format(BAD_TABLE),
                            (str(tweet_ts_ms),
                             unicode(data),
                             unicode(error_msg)))
    except (Exception, psycopg2.DatabaseError) as error:
        print error
    finally:
        if conn is not None:
            cur.close()
            conn.commit()
            conn.close()


def main():
    """Docstring"""
    try:
        (conn, cur) = connect_db("localhost", DB_CONFIG['name'],
                                 DB_CONFIG['user'],
                                 DB_CONFIG['password'])
        (last_json, last_dir) = get_last_json(cur, "meta_data")

        print "Writting to Database..."
        conn_string = ('postgresql+psycopg2://' + DB_CONFIG['user'] + ':'
                       + DB_CONFIG['password'] + '@localhost:5432/'
                       + DB_CONFIG['name'])
        engine = create_engine(conn_string)
        # TODO: check this dirs_2
        dirs = [x for x in os.listdir(DATA_PATH)
                if os.path.isdir(os.path.join(DATA_PATH, x))]
        # dirs = filter(lambda x: os.path.isdir(os.path.join(DATA_PATH, x)),
        # os.listdir(DATA_PATH))
        dirs.sort()
        for directory in dirs:
            jsons = [x.split('.')[0]
                     for x in os.listdir(os.path.join(DATA_PATH, directory))]
            jsons.sort()
            for json_file in jsons:
                if (int(directory) >= int(last_dir)
                        and int(json_file) > int(last_json)):
                    print "Consume the file {0} ".format(
                        os.path.join(DATA_PATH, directory, json_file)
                        + ".json")
                    update_metadata(cur, json_file, directory)
                    full_path = (os.path.join(DATA_PATH, directory, json_file)
                                 + ".json")
                    add_json_to_db(full_path, engine)
                    conn.commit()
        print "Wrote to Database."
    except KeyboardInterrupt:
        pass
    except (Exception, psycopg2.DatabaseError) as error:
        print error
    finally:
        if conn is not None:
            cur.close()
            conn.commit()
            conn.close()


if __name__ == "__main__":
    main()
