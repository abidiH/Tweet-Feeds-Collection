import facebook
import requests
import json
import os

from webhdfs.webhdfs import WebHDFS
# update your hosts' IP and port
webhdfs_client = WebHDFS("192.168.189.131", 50070, "cloudera")

access_token = '1657966841143723|9adpGe8H7zsKRH3JR-gJgpo6HY8'
graph_api = facebook.GraphAPI(access_token)
user = 'anyuser'

HDFS_FILE_PATH = '/user/cloudera/feeds/'
LOCAL_FILE_PATH ='feed.json'
webhdfs_client.mkdir(HDFS_FILE_PATH)

# to check if the file exceeds the max size , refresh the contents of the existing
max_file_size = 50000


class FacebookHandler():
    def dump_feed_hdfs(self, data):

        data_str = json.dumps(data)
        """  copy each json to hdfs """
        with open(LOCAL_FILE_PATH, 'a+') as f:
            # empty the contents of file if it exceeds the file size
            if os.stat(f.name).st_size > max_file_size:
                f.seek(0)
                f.truncate()
            f.write(data_str + '\n')
        webhdfs_client.copyFromLocal(LOCAL_FILE_PATH,
                                     HDFS_FILE_PATH + LOCAL_FILE_PATH)

    def get_feeds(self):
        print 'connecting to Facebook'
        profile = graph_api.get_object(user)
        profile_id = profile['id']
        args = {'fields' : 'id,name,message,created_time,comments{message,id,created_time,comment_count,from},from,to,likes{id,name}'}
        feeds = graph_api.get_connections(profile_id,'feed', **args)

        while True:
            try:
                # Perform some action on each feed in the collection we receive from
                # Facebook.
                for data in feeds['data']:
                    self.dump_feed_hdfs(data)
                # Attempt to make a request to the next page of data, if it exists.
                feeds = requests.get(feeds['paging']['next']).json()
            except KeyError:
                # When there are no more pages (['paging']['next']), break from the
                # loop and end the script.
                break

if __name__ == '__main__':
    fb = FacebookHandler()
    fb.get_feeds()
