import sys
import json
import pandas as pd
import re
import string

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
    filter out tweets that are re-tweet

Parameters
----------
tweet : string

Returns
-------
true or false: boolean
"""
def is_retweet_or_malformed_tweet(tweet):
    return bool(tweet.startswith("rt"))


"""
Function
--------
populate_term_scores
    associate a term with it's score

Parameters
----------
tweet_file : string

Returns
-------
scores: dictionary
"""
def  populate_term_scores(sent_file):
     scores = {} # initialize an empty dictionary
     for line in sent_file:
          term, score  = line.split("\t")  # The file is tab-delimited. "\t" means "tab character"
          scores[term] = int(score)  # Convert the score to an integer.
     return scores


def main():

    #open the sentiment and tweet files
    sent_file = open(sys.argv[1])
    tweet_file = open(sys.argv[2])

    # initialize an empty dictionary
    scores = populate_term_scores(sent_file)
    tweets_data = load_tweets(tweet_file)

    #create a data frame to hold the tweets and the language
    tweets = pd.DataFrame()

    #Ensure they are unicode and encoded and changed to lower
    tweets['text'] = [tweet.get('text','').encode("utf-8").lower() for tweet in tweets_data]
    tweets['lang'] = [tweet.get('lang','') for tweet in tweets_data]

    #query english only tweets
    tweets_filter = tweets.query('lang == "en"')

    new_sentiments = {} #dictionary to hold new sentiments words

    for index, row in tweets_filter.iterrows():
        tweet = row['text']
        tweet_score  = 0
        #select only the ascii and not a retweet
        if not is_retweet_or_malformed_tweet(tweet) and is_ascii(tweet):
            for word in tweet.split(' '):
                try:
                    tweet_score += scores[word]
                except KeyError:
                    pass #this word does not have a score
            #assign sentiment
            for word in tweet.split(' '):
                data = new_sentiments.get(word)
                if data == None:
                    new_sentiments[word] = [1, tweet_score]
                else:
                    new_sentiments[word] = [data[0]+1, data[1] + tweet_score]


    #end looping through the tweets
    for term in new_sentiments:
        score = str(new_sentiments[term][1]/new_sentiments[term][0])
        #remove any form of punctuation before printing out the term
        term = ''.join([c for c in term if c not in ('!', '?', ',', '.', ';')])
        print term + " " + score

if __name__ == '__main__':
    main()
