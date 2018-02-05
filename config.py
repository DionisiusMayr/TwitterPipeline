"""
Twitter authentication keys and base directories.
"""

import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(ROOT_DIR, 'data')

TWITTER_AUTH = {
    'consumer_key': '######',
    'consumer_secret': '######',
    'access_token_key': '######',
    'access_token_secret': '######'
}

DB_CONFIG = {
    'host': 'localhost',
    'user': 'db username',
    'name': 'db name',
    'password': 'db password'
}
