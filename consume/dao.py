"""Module for handling database access and running queries.
"""
import json
import re
import pandas as pd
import psycopg2

from sqlalchemy import create_engine

PREFIX = 'c_'


class PostgresDAO(object):
    """Docstring"""

    def __init__(self, db_config, table, bad_table, meta_data_table):
        """Docstring"""
        self.conn = psycopg2.connect(host=db_config['host'],
                                     database=db_config['name'],
                                     user=db_config['user'],
                                     password=db_config['password'])
        self.cur = self.conn.cursor()
        self.table = table
        self.bad_table = bad_table
        self.meta_data_table = meta_data_table
        self._conn_string = ('postgresql+psycopg2://' + db_config['user'] + ':'
                             + db_config['password'] + '@localhost:5432/'
                             + db_config['name'])
        self.engine = create_engine(self._conn_string)

    def update_metadata(self, last_json, last_dir):
        """Docstring"""
        self.cur.execute(u"""UPDATE meta_data SET last_json = {0}, """
                         u"""last_dir = {1}""".format(last_json, last_dir))
        if not self.cur.rowcount:
            self.cur.execute(u"""INSERT INTO meta_data (last_json, last_dir)
                    VALUES ('{0}', '{1}')""".format(last_json, last_dir))

    def get_last_json(self):
        """Docstring"""
        self.cur.execute(u"""CREATE TABLE IF NOT EXISTS meta_data """
                         u"""(last_json VARCHAR, last_dir VARCHAR)""")
        self.cur.execute(u"""SELECT last_json, last_dir """
                         u"""FROM {}""".format(self.meta_data_table))
        if self.cur.rowcount:
            return self.cur.fetchone()
        else:
            return ("0", "0")

    def table_exists(self, table_name):
        """
        Docstring
        """
        self.cur.execute(u"""SELECT table_name """
                         u"""FROM information_schema.tables """
                         u"""WHERE table_name='{0}'""".format(table_name))
        return self.cur.rowcount > 0

    def col_exists(self, table_name, col_name):
        """Docstring"""
        self.cur.execute(u"""SELECT column_name """
                         u"""FROM information_schema.columns """
                         u"""WHERE table_name='{0}' and """
                         u"""column_name='{1}'""".format(table_name, col_name))
        self.conn.commit()
        return self.cur.rowcount > 0

    def add_new_column(self, table_name, col_name, col_type):
        """Docstring"""
        # Checking for existing columns
        if not self.col_exists(table_name, col_name):
            print " Adding new column {0} with type {1}...".format(
                col_name, col_type)
            self.cur.execute(u"""ALTER TABLE IF EXISTS {0} """
                             u"""ADD COLUMN {1} {2}""".format(
                                 self.table, col_name, col_type))
            self.conn.commit()

    @staticmethod
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

    @staticmethod
    def dtype_to_psql(dtype):
        """
        # https://www.postgresql.org/docs/9.5/static/datatype.html
        # https://pandas.pydata.org/pandas-docs/stable/basics.html#dtypes
        Docstring
        """
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

    @staticmethod
    def get_df_columns(data_frame):
        """
        Docstring
        """
        return zip(list(data_frame.columns.values.tolist()),
                   data_frame.dtypes)

    @staticmethod
    def convert_types(data_frame):
        """
        Docstring
        """
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
                    print("Warning: untreated type:", type(row_data),
                          row_data)

    def workaround_type(self, table_name, data_frame):
        """
        TODO: Remove this (converts every column to varchar)
        """
        for col_name in list(data_frame.columns.values.tolist()):
            self.cur.execute(u"""ALTER TABLE {0} """
                             u"""ALTER COLUMN {1} TYPE {2}""".format(
                                 table_name, col_name, "varchar"))
            self.conn.commit()

    def create_table_from_df(self, data_frame, table_name, keys):
        """
        Docstring
        """
        print "Creating table {}...".format(table_name)
        schema_query = pd.io.sql.get_schema(frame=data_frame,
                                            name=table_name,
                                            con=self.conn,
                                            keys=keys)
        print "DATA_FRAME", '\n', data_frame
        print "DTYPES", '\n', data_frame.dtypes
        print "SCHEMA QUERY", '\n', schema_query
        self.cur.execute(schema_query)
        self.conn.commit()
        print "Table {0} created.".format(self.table)

    @staticmethod
    def get_tweet_timestamp(tweet):
        """
        Docstring
        """
        pattern = re.compile(r".*?\"timestamp_ms\":\"(\d+)\",?.*?")
        return pattern.match(tweet).group(1)

    def add_json_to_db(self, json_path):
        """
        Docstring
        """
        tweet = None
        with open(json_path, 'r') as jfile:
            jdata = jfile.read()
            tweet = json.loads(jdata)
        data_frame = pd.DataFrame.from_dict([tweet])
        PostgresDAO.convert_types(data_frame)

        # Add columns to Database, if necessary
        col_list = [PREFIX + PostgresDAO.shorten_name(str(col))
                    for col in list(data_frame.columns.values.tolist())]
        data_frame.columns = col_list

        if not self.table_exists(self.table):
            self.create_table_from_df(data_frame, self.table, PREFIX + 'id')
            self.workaround_type(self.table, data_frame)
        else:
            for (col_name, col_type) in PostgresDAO.get_df_columns(data_frame):
                converted_col_type = PostgresDAO.dtype_to_psql(str(col_type))
                self.add_new_column(self.table, col_name, converted_col_type)

            self.workaround_type(self.table, data_frame)

        try:
            with self.engine.connect() as e_conn, e_conn.begin():
                data_frame.to_sql(self.table, e_conn, index=False,
                                  if_exists='append')
        except psycopg2.IntegrityError as error:
            tweet_ts_ms = PostgresDAO.get_tweet_timestamp(tweet)
            error_msg = str(error).split('\n')[0]
            print("Bad data: ts_ms:", tweet_ts_ms, "Error:", error_msg)

            if not self.table_exists(self.bad_table):
                print "Creating table {0}...".format(self.bad_table)
                self.cur.execute(u"""CREATE TABLE IF NOT EXISTS {0} ("""
                                 u"""ts_ms VARCHAR, """
                                 u"""raw_data VARCHAR, """
                                 u"""error_msg VARCHAR"""
                                 u""")""".format(self.bad_table))
                self.conn.commit()
                print "Table {0} created.\n".format(self.bad_table)

            self.cur.execute(u"""INSERT INTO %s (ts_ms, raw_data, error_msg)"""
                             u"""VALUES (%s, %s, %s)""", (self.bad_table,
                                                          str(tweet_ts_ms),
                                                          unicode(tweet),
                                                          unicode(error_msg)))

    def dispose(self):
        """
        Docstring
        """
        if self.conn is not None:
            self.cur.close()
            self.conn.commit()
            self.conn.close()
