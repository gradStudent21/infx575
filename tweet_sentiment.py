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
populate_term_scores
    associate a term with it's score

Parameters
----------
sent_file : string

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

"""
Function
--------
calculate_sentiment_scores
    returns the score associated with the terms found in the tweet

Parameters
----------
scores: dictionary
tweet:  string

Returns
-------
sentiment: either 0 or a score associated with the selected words
"""
def calculate_sentiment_scores(scores, tweet):
    sentiment = 0 # reset sentiment score
    b = r'(\s|^|$)'
    for term, score in scores.items():
        if re.search(r'\b' + term + r'\b', tweet, flags=re.IGNORECASE):
            sentiment += score
    return sentiment


def main():

    #open the sentiment and tweet files
    sent_file = open(sys.argv[1])
    tweet_file = open(sys.argv[2])

    #get a dictionary of terms and related sentiments' score
    scores = populate_term_scores(sent_file)
    tweets_data = load_tweets(tweet_file)

    #create a data frame to hold the tweets and the language
    tweets = pd.DataFrame()
    tweets['text'] = [tweet.get('text','').encode("utf-8").lower() for tweet in tweets_data]
    tweets['lang'] = [tweet.get('lang','') for tweet in tweets_data]

    #query english only tweets
    tweets_filter = tweets.query('lang == "en"')

    #iterate through the dataframe get the text attribute and calculate the sentiment for the tweet
    for index, row in tweets_filter.iterrows():
        tweet = row['text']
        if is_ascii(tweet): #select only the ascii and not a retweet
            #calculate sentiment for a tweet
            sentiment = calculate_sentiment_scores(scores, tweet)
        else:
            continue
        #print out the tweet and the sentiment
        print tweet, sentiment
    #end iteration

if __name__ == '__main__':
    main()
