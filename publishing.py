import requests
from google.auth import crypt
from google.auth import jwt
import time
import os
import io
import json
import numpy as np
import sys
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub import PublisherClient
import threading
#import pub.py
#from pub.py import w 
#credential_path = ""
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
#project_id = "dn-us-analytics-dev"
#topic_id = "pubsub_log"
#publisher = ""
#topic_path = ""
count = 0

def func1(w):
    #global info
    #info = w
    #print(info)
    global project_id
    global topic_id
    global publisher
    global topic_path
    with open("pubsub_loggs.json") as f1:
        inf = json.load(f1)
    credential_path = inf['project']['credentials']
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    project_id = inf['project']['project_id']
    topic_id = inf['project']['topic_id']
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    public(w)


def public(lst):
    global count
    global publisher
    global topic_path
    #print("here")
    for k in lst:
        #print(i)
        data = f"{k}"
        # Data must be a bytestring
        data = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        try:
            future = publisher.publish(topic_path, data)
            count = count + 1
            print(future.result())
        except:
            print('oops')
        #print(future.result())
        #count = count + 1


#w = [3,4,5,6,7,8,9]
#w = 4
#func1()


#if __name__ == "__main__":
    #publisher = pubsub_v1.PublisherClient()
    #topic_path = publisher.topic_path(project_id, topic_id)
    #func1()
    

#data = f"{yup}"
# Data must be a bytestring
#data = data.encode("utf-8")
# When you publish a message, the client returns a future.
#try:
    #future = publisher.publish(topic_path, data)
    #print(future.result())
#except:
    #print('oops')
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
#two_split = np.array_split(w,2)
#for arr in two_split:

#func1(publisher,topic_path)