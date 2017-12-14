#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 17:35:13 2017

@author: vinaya
"""

import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener




    # Read the credententials from 'twitter-app-credentials.txt' file
consumer_key = "jrPlepQGYmtZkO4locnUwawHe"
consumer_secret = "JWREIWrENWcGt37FEyTPhfE34j4O1w6kkF02wCUhLB28blZ0nq"
access_token = "899279922639675392-qWkTEtTiWJ6dYPrefliL21s2FkqWY6I"
access_secret = "K6rXIUlCMl7HUFYPIXvbgC14DY4LrLtgxjIbGh5aavZWN"


#TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

#Twitter Stream Listener
class KafkaPushListener(StreamListener):          
	def __init__(self):
		#localhost:9092 = Default Zookeeper Producer Host and Port Adresses
		self.client = pykafka.KafkaClient("localhost:9092")
		
		#Get Producer that has topic name is Twitter
		self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()
  
	def on_data(self, data):
		#Producer produces data for consumer
		#Data comes from Twitter
		self.producer.produce(bytes(data, "ascii"))
		return True
                                                                                                                                           
	def on_error(self, status):
		print(status)
		return True

#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

#Produce Data that has Game of Thrones hashtag (Tweets)
twitter_stream.filter(track=['#Trump'])
