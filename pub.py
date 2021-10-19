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
import datetime
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
with open("lright.json") as f1:
    data = json.load(f1)
# TODO(developer)
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"], 
    ssl_key = data["mysql"]["ssl_key"],ssl_cert = data["mysql"]["ssl_cert"],ssl_ca = data["mysql"]["ssl_ca"],db = data["mysql"]["db"])
count = 0
#file = open(data["file"]["file_name"],'r')
#f = file.readlines()
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
                start_time = datetime.datetime.now()
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
                a_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
                os.chdir(a_path)
                #print("in writing")
                cursor = mydb.cursor()
                a = "publish"
                s = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values ('" + a + "' ,'" + str(topic_id) + "', '" + str(start_time) + "', '" + str(datetime.datetime.now()) + "', " + str(count) + ")"
                #print(s)
                cursor.execute(s)
                mydb.commit()
                count = 0
        #if datetime.datetime.now().minute == 10:
            #count = 0
            #written = False