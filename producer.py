#!/usr/bin/env python
# coding: utf-8
# In[1]:
import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer
# twitter setup
consumer_key = "GLDmO41uBFoI1RYI2kRKRroBY"
consumer_secret = "hlapQbdT8PYZyq2NDNRuWTV91MSnWTNTHaKYqt4YFNCEouKdZS"
access_token = "1269902479732273152-MXftHSmTg9ysLvM6CCG2NeQOROx1Tg"
access_token_secret = "MooGpMKtOi5LanEYJ61EQNTXVitV0ylitrUigBjieUdLC"
# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object by passing in auth information
api = tweepy.API(auth)
from datetime import datetime, timedelta
def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=1)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'twitter_data'
def get_twitter_data():
    res = api.search("Apple OR iphone OR iPhone")
    for i in res:
        record = ''
        record += str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.user.followers_count)
        record += ';'
        record += str(i.user.location)
        record += ';'
        record += str(i.favorite_count)
        record += ';'
        record += str(i.retweet_count)
        record += ';'
        producer.send(topic_name,str.encode(record))
get_twitter_data()
def periodic_work(interval):
    while True:
        get_twitter_data()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)
periodic_work(60 * 0.1)  # get data every couple of minutes

