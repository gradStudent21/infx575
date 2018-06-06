import sys
import json
import pandas as pd
import re

#Pierre Augustamar - INFX575-A

"""
Function
--------
is_ascii
    check if a string is composed of ASCII characters

Parameters
----------
location : string

Returns
-------
 true or false: boolean
"""
def is_ascii(s):
    return bool(re.match(r'[\x00-\x7F]+$', s))

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
is_retweet_or_malformed_tweet
    filter out tweets that are re-tweet or have a www, http, or https label

Parameters
----------
tweet : string

Returns
-------
true or false: boolean
"""
def is_retweet_or_malformed_tweet(tweet):
    return bool(tweet.startswith("rt") or tweet.startswith("www") or
                tweet.startswith("http") or tweet.startswith("https"))


def main():

    #open the tweet file
    tweet_file = open(sys.argv[1])
    #dictionary to hold ocurrences to gather frequency
    occurence = {}
    #load tweets into a list
    tweets_data = load_tweets(tweet_file)
    #create a data frame to hold the tweets'text and language
    tweets = pd.DataFrame()
    tweets['text'] = [tweet.get('text','').encode("utf-8").lower() for tweet in tweets_data]
    tweets['lang'] = [tweet.get('lang','') for tweet in tweets_data]
    #query english related tweets
    tweets_filter = tweets.query('lang == "en"')

    #iterate through the list of tweets
    for index, row in tweets_filter.iterrows():
        tweet = row['text']
        #select only the ascii words and original tweets
        if is_ascii(tweet) and not is_retweet_or_malformed_tweet(tweet):
            for word in tweet.split(' '):
                try:
                    if occurence.get(word) is None:
                        occurence[word] = 1
                    else:
                        occurence[word] += 1
                except KeyError:
                    pass #this word does not have a score

    #end iteration

    #loop through the terms and calculate the frequncy
    for term in occurence:
        if term.strip() == "":
            pass
        else:
            print term.strip() + " %f" %(float(occurence[term])/float(len(occurence)))

if __name__ == '__main__':
    main()
