from google.cloud import pubsub_v1
import time
import json
import os
import os.path
from os import path
import csv
import datetime
import base64
import numpy as np
import threading
with open("lright.json") as f1:
    data = json.load(f1)
# TODO(developer)
t1 = datetime.datetime.now()
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
publish_futures=[]
count = 0
#file = open(data["file"]["file_name"],'r')
#f = file.readlines()
def public(lst):
    global count
    print("in here")
    for k in lst:
        #print(i)
        data = f"{k}"
        # Data must be a bytestring
        data = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        #try:
        future = publisher.publish(topic_path, data)
        count = count + 1
        #except:
            #print('oops')
        #print(future.result())
        #count = count + 1
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=data["project"]["credentials"]
publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)
more = ""
#path = "C:\\Users\\Prachetas Deshpande\\Documents\\deep-north-work\\cool_11.json"
#print(path)

the_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
rows = []
fields=[]
thread_list = []
stuff = ""
arr = ""
yipee = []
x = 8
#path = "C:\\Users\\Prachetas Deshpande\\Documents\\deep-north-work\\results\\result-20210814\\result-20210814\\counting"
    #if str(path.isdir(new_path))=="True":
#os.chdir(the_path)
the_path = "D:\\DoNotDelete\\Documents\\pub_files"
if str(path.isdir(the_path)) == "True":
    os.chdir(the_path)
    #print(len(os.listdir()))
    for i in os.listdir():
        with open(i,"r") as data_file:
            data = data_file.read()
        #print(data)
        content_list = data.split("\n")
        #content_list = list(content_list)
        two_split = np.array_split(content_list,2)
        for arr in two_split:
            public(arr)
            time.sleep(10.0)
start = time.time()
#info_dat = data["file"]["file_name"]
#with open(info_dat, "r") as data:
    #content = data.read()
#content_list = content.split("\n")
#cont = list(content_list)
#two_split = np.array_split(content_list,2)
#for arr in two_split:
    #public(arr)
    #time.sleep(30.0)
t2 = datetime.datetime.now()
#with open("pub_telemtery.txt","w") as p:
    #p.write(t1 + " " + t2 + "    " + count)
print("time taken is ",time.time()-start)
print(f"Published messages to {topic_path}.")
print(count)
#my_file = open("jik.txt", "r")
#content = my_file.read()
#content_list = content.split("\n")