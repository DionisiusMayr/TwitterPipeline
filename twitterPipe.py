import sys

import tweepy
import json
import pandas as pd

from twitter_keys import CONSUMER_KEY
from twitter_keys import CONSUMER_SECRET
from twitter_keys import ACCESS_TOKEN_KEY
from twitter_keys import ACCESS_TOKEN_SECRET

raw_data_path = 'data/data.txt'

class StreamListener(tweepy.StreamListener):

    def on_data(self, data):
        print data
        # Save raw_data
        raw_data_file = open(raw_data_path, "a")
        raw_data_file.write(data)
        raw_data_file.close()
        return True

    def on_error(self, status_code):
        print "Error: " + str(status_code)
        if status_code == 420:
            return False

    # def on_status(self, status):
        # print(status.text)

if len(sys.argv) > 1:
    if sys.argv[1] == "listener":
        auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth)

        # public_tweets = api.home_timeline()
        # for tweet in public_tweets:
            # print tweet.text

        # user = api.get_user(19701628)
        # print user

        streamListener = StreamListener()
        stream = tweepy.Stream(api.auth, streamListener)
        stream.filter(track=['python'])
    elif sys.argv[1] == "consume":
        raw_data_file = open(raw_data_path, "r")
        tweets = [json.loads(line) for line in raw_data_file]
        df = pd.DataFrame.from_dict(tweets)
        raw_data_file.close()

