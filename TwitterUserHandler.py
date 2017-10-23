import tweepy
import json
# update access tokens
access_token_secret = ''
consumer_key = ''
consumer_secret = ''
access_token = ''

screen_name = 'name'
LOCAL_FILE_PATH ='tweet.json'

def get_tweets():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    all_tweets = []
    new_tweets = api.user_timeline(screen_name= screen_name, count=200)
    write_to_hdfs(new_tweets)
    #followers = api.followers(screen_name = screen_name)
    all_tweets.extend(new_tweets)
    oldest = all_tweets[-1].id - 1
    while len(new_tweets) >0:
        new_tweets = api.user_timeline(screen_name=screen_name,count =200,max_id=oldest)
        write_to_hdfs(new_tweets)
        all_tweets.extend(new_tweets)
        oldest = all_tweets[-1].id -1
        print '%s tweets collected ' % (len(all_tweets))


def write_to_hdfs(tweets):
    for tweet in tweets:
        json_status = json.dumps(tweet._json)
        #print json_status
        #write the json_status to hdfs
        with open(LOCAL_FILE_PATH, 'a+') as f:
            f.write(json_status + '\n')


if __name__ == '__main__':
    get_tweets()

