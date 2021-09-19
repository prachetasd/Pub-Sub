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
# TODO(developer)
project_id = "dn-us-analytics-dev"
topic_id = "sensors"
def public(lst):
    for k in lst:
        #print(i)
        data = f"{k}"
        # Data must be a bytestring
        data = data.encode("utf-8")
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)
        #print(future.result())
        #count = count + 1

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)
more = ""
#path = "C:\\Users\\Prachetas Deshpande\\Documents\\deep-north-work\\cool_11.json"
#print(path)
count = 0
the_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
rows = []
fields=[]
thread_list = []
stuff = ""
arr = ""
yipee = []
x = 8
#for i in range(1077,1078):
    #if i!=1079 or i!=1101 or i!=1102 or i!=1103 or i!=1104 or i!=1105:
    #new_path = the_path + str(i)
        #print(new_path)
path = "C:\\Users\\Prachetas Deshpande\\Documents\\deep-north-work\\results\\result-20210814\\result-20210814\\counting"
    #if str(path.isdir(new_path))=="True":
os.chdir(the_path)
start = time.time()
for file in os.listdir():
    # Check whether file is in text format or not
    if file.endswith("1.csv"):
        file_path = f"{the_path}\\{file}"
        with open(file_path, 'r') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            # extracting field names through first row
            fields = next(csvreader)
            # extracting each data row one by one
            for row in csvreader:
                rows.append(row)
        #for row in rows:
            # parsing each column of a row
            #for j in range(len(row)):
                #if j==len(row)-1:
                    #row[j]=datetime.datetime.strptime(row[j], "%m/%d/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")
        #yipee = rows[:10000]
        eight_split = np.array_split(rows,8)
        #list_of_lists = [rows[i:i+x] for i in range(0, len(rows), x)]
        #print(list_of_lists[0][0])
        for array in eight_split:
            #print(list(array))
            #public(eight_split[0])
            t1 = threading.Thread(target = public,args=[array])
            t1.start()
            #thread_list.append(t1)
        #for t in thread_list:
            #t.start()
        more=""
    

print("time taken is ",time.time()-start)
print(f"Published messages to {topic_path}.")
print(count)