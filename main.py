from twitter_client.twitter_client import TwitterClient
from kafka.kafka_producer import send
import json
import os
from dotenv import load_dotenv

load_dotenv()

token = os.environ['TWITTER_API_TOKEN']
key = os.environ['TWITTER_API_KEY']
secret = os.environ['TWITTER_API_SECRET']

client = TwitterClient(key=key, secret=secret)
resp = client.get_user_id('TheStalwart', token=token)
user=resp.json()['data']

print(user)
tweets=client.get_user_timeline(user['id'], token).json()['data']
print(tweets)

for i in range(0, len(tweets)):
    item=tweets[i]
    print(item)
    # print(type(item))
    send('tweets', None, item)
