"""
Listen to real time tweets filtering only the tweets that contain an specific
word.
"""

import tweepy

from ..config import TWITTER_AUTH
from .stream_listener import StreamListener


def main():
    """Docstring"""
    stream = None
    try:
        auth = tweepy.OAuthHandler(TWITTER_AUTH['consumer_key'],
                                   TWITTER_AUTH['consumer_secret'])
        auth.set_access_token(TWITTER_AUTH['access_token_key'],
                              TWITTER_AUTH['access_token_secret'])
        api = tweepy.API(auth)

        stream_listener = StreamListener()
        stream = tweepy.Stream(api.auth, stream_listener)
        stream.filter(track=['python'])
    except KeyboardInterrupt:
        pass
    finally:
        if stream is not None:
            stream.disconnect()


if __name__ == "__main__":
    main()
