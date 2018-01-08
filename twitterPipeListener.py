import tweepy

from secret_keys import CONSUMER_KEY
from secret_keys import CONSUMER_SECRET
from secret_keys import ACCESS_TOKEN_KEY
from secret_keys import ACCESS_TOKEN_SECRET

RAW_DATA_PATH = 'data/data.txt'

class StreamListener(tweepy.StreamListener):

    def on_data(self, data):
        print data
        # Save raw data
        raw_data_file = open(RAW_DATA_PATH, "a")
        raw_data_file.write(data)
        raw_data_file.close()
        return True

    def on_error(self, status_code):
        print "Error: " + str(status_code)
        if status_code == 420:
            return False

    # def on_status(self, status):
        # print(status.text)

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
