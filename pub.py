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
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
publish_futures=[]
count = 0
total = 0
written = False
#file = open(data["file"]["file_name"],'r')
#f = file.readlines()
def public(lst):
    global count
    global total
    start = time.time()
    #print("here")
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
    total = total + count
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
    while True:
        os.chdir(the_path)
        if len(os.listdir())!=0:
            for i in os.listdir():
                try:
                    with open(i,"r") as data_file:
                        data = data_file.read()
                except PermissionError as e:
                    continue
                #print(data)
                content_list = data.split("\n")
                #content_list = list(content_list)
                two_split = np.array_split(content_list,2)
                for arr in two_split:
                    public(arr)
                    time.sleep(10.0)
                data_file.close()
                os.chdir(the_path)
                os.remove(i)
        if datetime.datetime.now().minute == 28:
            a_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
            os.chdir(a_path)
            #print("in writing")
            #with open("dem.txt","w") as dats:
            file1 = open("dem.txt","w")
            file1.write("For topic " + topic_id + " company_id:1, property_id:2 " + str(datetime.datetime.now()-datetime.timedelta(hours = 1)) 
            + " - " + str(datetime.datetime.now()) + "        " + str(count))
            file1.close()
            #written = True
            #count = 0
        #if datetime.datetime.now().minute == 10:
            #count = 0
            #written = False