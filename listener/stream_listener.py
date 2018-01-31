"""
Module to listen real time tweets.
It stores each tweet in a separate file, grouping them in one folder per hour.
"""

import re
import os
from time import gmtime, strftime

import tweepy
from ..config import DATA_PATH


class StreamListener(tweepy.StreamListener):
    """Real time tweet listener."""

    def on_data(self, data):
        # Groups tweets by hour in a folder
        cur_time = strftime("%Y%m%d%H", gmtime())
        folder_name = os.path.join(DATA_PATH, cur_time)

        # Regex to extract the id of a tweet
        pat = re.compile(r".*?\"id\":(\d+),.*?")
        tweet_id = pat.match(data).group(1)
        full_path = os.path.join(folder_name, str(tweet_id)) + ".json"

        if not os.path.exists(folder_name):
            os.mkdir(folder_name)

        print "Storing {0}... to {1}".format(data[:45], full_path)
        with open(full_path, 'w') as open_file:
            open_file.write(data)

        return True

    def on_error(self, status_code):
        print "Error: " + str(status_code)
        if status_code == 420:
            return False
