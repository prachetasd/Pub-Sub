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

count = 0

def public(lst):
    global count
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
        except:
            print('oops')
        #print(future.result())
        #count = count + 1

#w = [3,4,5,6,7,8,9]
#w = 4
yup = ('publish' ,'sensors', '2021-10-26 09:26:42.865170', '2021-10-26 09:27:02.952615', 1104)
credential_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try\\pubsub_logs.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = "dn-us-analytics-dev"
topic_id = "pubsub_log"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)
data = f"{yup}"
# Data must be a bytestring
data = data.encode("utf-8")
# When you publish a message, the client returns a future.
try:
    future = publisher.publish(topic_path, data)
    print(future.result())
except:
    print('oops')
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
#two_split = np.array_split(w,2)
#for arr in two_split:

#func1(publisher,topic_path)