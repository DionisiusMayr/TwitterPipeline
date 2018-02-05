"""
Dosctring
"""

import os
# import logging
import psycopg2
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from ..config import DB_CONFIG
from ..config import DATA_PATH
# from twitter_pipeline.consume.parser import Parser
from twitter_pipeline.consume.dao import PostgresDAO

TABLE = "tweets"
BAD_TABLE = "bad_tweets"
PREFIX = 'c_'


def should_consume(cur_dir, cur_file, last_dir, last_file):
    """ Should only consume files that are newer than the last consumed
    files.
    """
    return int(cur_dir) >= int(last_dir) and int(cur_file) > int(last_file)


def get_comp_json_path(directory, jfile):
    """ Generates the complete path to the json file.
    """
    return os.path.join(DATA_PATH, directory, jfile) + ".json"


def get_files_to_consume(base_path, last_dir, last_json):
    """Docstring"""
    to_consume = []
    dirs = [d for d in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, d))]
    dirs.sort()

    for directory in dirs:
        json_files = [d.split('.')[0] for d
                      in os.listdir(os.path.join(base_path, directory))]
        json_files.sort()

        for jfile in json_files:
            if should_consume(directory, jfile, last_dir, last_json):
                to_consume.append(get_comp_json_path(directory, jfile))

    to_consume.sort()
    return to_consume


def main(argv=None):
    """Docstring"""

    def helper_function(jfile):
        """Docstring"""
        dao = PostgresDAO(DB_CONFIG, "tweets", "bad_tweets", "meta_data")
        dao.add_json_to_db(jfile)
        dao.update_metadata(jfile)
        dao.dispose()

    print "Executing module consume..."

    # parser = Parser(argv)
    # known_args, pipeline_args = parser.get_args()

    last_json, last_dir = 0, 0
    try:
        dao = PostgresDAO(DB_CONFIG, "tweets", "bad_tweets", "meta_data")
        (last_json, last_dir) = dao.get_last_json()
    except psycopg2.DatabaseError as error:
        print error
    finally:
        dao.dispose()

    files_to_consume = get_files_to_consume(DATA_PATH, last_dir, last_json)
    print "Found {0} files to consume.".format(len(files_to_consume))

    if len(files_to_consume) > 0:
        pipe = beam.Pipeline(options=PipelineOptions())
        # lines = pipe | 'ReadMyFile' >> beam.io.ReadFromText(known_args.input)
        # lines = pipe | 'Create' >> beam.Create(['test', 'cat', 'test', 'apple'])
        lines = pipe | 'Create' >> beam.Create(files_to_consume)
        counts = (
            # lines
            # | 'Split' >> (beam.FlatMap(lambda x: x.split(' '))
            #               .with_output_types(unicode))
            # | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            # | 'GroupAndSum' >> beam.CombinePerKey(sum)
            lines | 'Description' >> beam.ParDo(helper_function)
            )

        # counts | 'Print' >> beam.ParDo(lambda (w, c): printar('%s: %s' % (w, c)))
        # counts | 'PrintType' >> beam.ParDo(lambda (w, c): printar(type(c)))
        # output = counts | 'Format' >> beam.Map(format_result)
        # output | 'Write' >> beam.io.WriteToText(known_args.output)

        result = pipe.run()
        result.wait_until_finish()
    print "Finished module consume."


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
