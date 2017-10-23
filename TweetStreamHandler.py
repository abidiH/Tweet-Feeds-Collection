import tweepy
#from webhdfs.webhdfs import WebHDFS
from time import sleep

import os
# update your hosts' IP and port
#webhdfs_client = WebHDFS("192.168.189.131", 50070, "cloudera")

# update twitter access tokens
access_token_secret = ''
consumer_key = ''
consumer_secret = ''
access_token = ''


FILTER_WORDS_LIST = ['love']
HDFS_FILE_PATH = '/user/cloudera/tweets/'
LOCAL_FILE_PATH ='streaming-tweets.json'
#webhdfs_client.mkdir(HDFS_FILE_PATH)

# to check if the file exceeds the max size , refresh the contents of the existing
max_file_size = 50000


class TwitterHandler(tweepy.StreamListener):

    def on_data(self, data):
        with open(LOCAL_FILE_PATH, 'a+') as f:
            # empty the contents of file if it exceeds the file size
            #if os.stat(f.name).st_size > max_file_size:
                #f.seek(0)
                #f.truncate()
            #print data
            f.write(data + '\n')
        #webhdfs_client.copyFromLocal(LOCAL_FILE_PATH,
                                     #HDFS_FILE_PATH + LOCAL_FILE_PATH)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    tp = TwitterHandler()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = tweepy.Stream(auth, tp)
    #sleep(1)
    stream.filter(track=FILTER_WORDS_LIST)

