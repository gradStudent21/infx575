import sys
import json
import pandas as pd
import re
import operator

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
populate_term_scores
    associate a term with it's score

Parameters
----------
sentiments_file : string

Returns
-------
scores: dictionary
"""
def  populate_term_scores(sent_file):
     scores = {}
     for line in sent_file:
          term, score  = line.split("\t")
          scores[term] = int(score)
     return scores

"""
Function
--------
get_user_location
    filter the location associated with a tweet using the user's field

Parameters
----------
tweet : string

Returns
-------
location: string
"""
def get_user_location(tweet):
    user = tweet.get('user', {})
    location = user.get('location', ' ')
    return location


"""
Function
--------
filter_us_locations
    filter US specific locations.

Parameters
----------
location : string

Returns
-------
location: string
"""
def filter_us_locations(location):
    if location == None:
        return None
    else:
        if ", USA" in location.encode("utf-8"):
            return location

"""
Function
--------
load_tweets_by_states
    map each states with the related tweets

Parameters
----------
tweets_data: list
state_names_to_abbrevs:  dictionary

Returns
-------
tweets_by_states: dictionary
"""
def load_tweets_by_states(tweets_data, state_names_to_abbrevs):

    state_abbrevs = state_names_to_abbrevs.values()
    tweets_by_states = dict([(abbrev, 'None') for abbrev in state_abbrevs])
    for tweet in tweets_data:
        location = get_user_location(tweet)
        us_local = filter_us_locations(location)
        if  us_local is None:
            continue
        for name, abbrev in state_names_to_abbrevs.items():
            if us_local.upper().find(name) > -1:
                tweets_by_states[abbrev] = tweet['text'].encode("utf-8")
                break
            if re.findall(r'\b(' + abbrev + r')\b', us_local, re.IGNORECASE):
                tweets_by_states[abbrev] = tweet['text'].encode("utf-8")

    return tweets_by_states


"""
Function
--------
get_sentiment_score
    returns the score associated with a word foudn in the tweet

Parameters
----------
scores: dictionary
word:  string

Returns
-------
 either 0 or a score associated with the selected word
"""
def get_sentiment_score(scores, word):
    for term, score in scores.items():
        if re.search(r'\b' + term + r'\b', word):
            return score
    return 0

#dictionary mappying abreviated and actual state's name
state_names_to_abbrevs = \
    dict([
    ('ALABAMA', 'AL'),
    ('ALASKA', 'AK'),
    ('ARIZONA', 'AZ'),
    ('ARKANSAS', 'AR'),
    ('CALIFORNIA', 'CA'),
    ('COLORADO', 'CO'),
    ('CONNECTICUT', 'CT'),
    ('DELAWARE', 'DE'),
    ('FLORIDA', 'FL'),
    ('GEORGIA', 'GA'),
    ('HAWAII', 'HI'),
    ('IDAHO', 'ID'),
    ('ILLINOIS', 'IL'),
    ('INDIANA', 'IN'),
    ('IOWA', 'IA'),
    ('KANSAS', 'KS'),
    ('KENTUCKY', 'KY'),
    ('LOUISIANA', 'LA'),
    ('MAINE', 'ME'),
    ('MARYLAND', 'MD'),
    ('MASSACHUSETTS', 'MA'),
    ('MICHIGAN', 'MI'),
    ('MINNESOTA', 'MN'),
    ('MISSISSIPPI', 'MS'),
    ('MISSOURI', 'MO'),
    ('MONTANA', 'MT'),
    ('NEBRASKA', 'NE'),
    ('NEVADA', 'NV'),
    ('NEW HAMPSHIRE', 'NH'),
    ('NEW JERSEY', 'NJ'),
    ('NEW MEXICO', 'NM'),
    ('NEW YORK', 'NY'),
    ('NORTH CAROLINA', 'NC'),
    ('NORTH DAKOTA', 'ND'),
    ('OHIO', 'OH'),
    ('OKLAHOMA', 'OK'),
    ('OREGON', 'OR'),
    ('PENNSYLVANIA', 'PA'),
    ('RHODE ISLAND', 'RI'),
    ('SOUTH CAROLINA', 'SC'),
    ('SOUTH DAKOTA', 'SD'),
    ('TENNESSEE', 'TN'),
    ('TEXAS', 'TX'),
    ('UTAH', 'UT'),
    ('VERMONT', 'VT'),
    ('VIRGINIA', 'VA'),
    ('WASHINGTON', 'WA'),
    ('WEST VIRGINIA', 'WV'),
    ('WISCONSIN', 'WI'),
    ('WYOMING', 'WY')
 ])


def main():

    #open the sentiment and tweet files
    sent_file = open(sys.argv[1])
    tweet_file = open(sys.argv[2])
    # initialize an empty dictionary
    sentiment_by_states = {}
    # get the scores related to each of the collected sentiments
    scores = populate_term_scores(sent_file)
    #load tweets from the collected output file
    tweets_data = load_tweets(tweet_file)
    #map each states with the related tweet
    tweets_by_states = load_tweets_by_states(tweets_data, state_names_to_abbrevs)
    #iterate through the list of states and associated tweets
    for key, value in tweets_by_states.items():
        tweet_score = 0 #initialize the tweet score for each state
        #continue to the next state if current state has no tweet
        if value is None:
            continue
        #take each of the words in the tweet and calculate the sentiment score
        for word in value.split(' '): #
            try:
                tweet_score = tweet_score + get_sentiment_score(scores, word)
            except KeyError:
                pass
        #map the state with the tweet's score
        sentiment_by_states[key] = tweet_score

    #filter out the state with the highest sentiment's score
    happiest_state = max(sentiment_by_states.iteritems(), key=operator.itemgetter(1))[0]
    print happiest_state

if __name__ == '__main__':
    main()
