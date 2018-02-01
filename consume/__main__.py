"""
Dosctring
"""

import os
import psycopg2
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from twitter_pipeline.consume.dao import PostgresDAO
from ..config import DB_CONFIG
from ..config import DATA_PATH


TABLE = "tweets"
BAD_TABLE = "bad_tweets"
PREFIX = 'c_'


def main():
    """Docstring"""
    pipe = beam.Pipeline(options=PipelineOptions())
    print "Running module consume..."
    try:
        dao = PostgresDAO(DB_CONFIG, "tweets", "bad_tweets", "meta_data")
        (last_json, last_dir) = dao.get_last_json()

        dirs = [x for x in os.listdir(DATA_PATH)
                if os.path.isdir(os.path.join(DATA_PATH, x))]
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
                    dao.update_metadata(json_file, directory)
                    full_path = (os.path.join(DATA_PATH, directory, json_file)
                                 + ".json")
                    dao.add_json_to_db(full_path)
    except KeyboardInterrupt:
        pass
    except psycopg2.DatabaseError as error:
        print error
    finally:
        dao.dispose()
    print "Finished module consume."


if __name__ == "__main__":
    main()
