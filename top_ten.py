import sys
import json
import re

#Pierre Augustamar - INFX575-A

"""
Function
--------
load_tweets
    load tweets taken from the stream collected through the Twitter's API

Parameters
----------
tweet_file : string

Returns
-------
tweets_data: list
"""
def load_tweets(tweet_file):
    tweets_data = []
    for line in tweet_file:
        try:
           tweet = json.loads(line)
           tweets_data.append(tweet)
        except:
            continue
    return tweets_data


"""
Function
--------
get_hashtags
    filter out the text entry from the hashtag attribute of the tweet

Parameters
----------
tweet_file : string

Returns
-------
hashtag: list
"""
def get_hashtags(tweet):
    entities = tweet.get('entities', {})
    if entities == {}:
        return None
    hashtags = entities.get('hashtags', [])
    if hashtags == []:
        return None

    return [hashtag['text'].encode("utf-8").lower() for hashtag in hashtags]


def main():

    #open the sentiment and tweet files
    tweet_file = open(sys.argv[1])
    #load all the tweets into a list
    tweets_data = load_tweets(tweet_file)
    #dictionary to hold hashtag entries
    hashtag_entries = {}

    #iterate through the list of tweets and add hashtags with related frequency into the dictionary
    for tweet in tweets_data:
        hashtags = get_hashtags(tweet)
        if hashtags is None:
            continue
        for tag in hashtags:
            if tag in hashtag_entries:
                hashtag_entries[tag] += 1
            else:
                hashtag_entries[tag] = 1
    #end iteration

    #sort the keys in descending order
    hashtags_sorted_keys = sorted(hashtag_entries, key=hashtag_entries.get, reverse=True)
    counter = 0

    #iterate through the hashtag dictionary and prints the first 10 entries
    for r in hashtags_sorted_keys:
        if counter == 10:
            break
        print r, hashtag_entries[r]
        counter = counter + 1
    #end iteration

if __name__ == '__main__':
    main()
