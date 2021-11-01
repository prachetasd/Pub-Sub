from google.cloud import pubsub_v1
import time
import json
import os
import os.path
from os import path
import sys
import csv
import datetime
import base64
import numpy as np
import threading
import datetime
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import subprocess
#import execfile
import publishing
with open("config_pub_generic.json") as f1:
    data = json.load(f1)
#with open("config_sub_generic.json") as f1:
    #data_ok = json.load(f1)
# TODO(developer)
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
file_read_path = data["project"]["Files_to_read"]
files_read = data["project"]["Files_read"]
#mydb = mysql.connector.connect(host=data_ok["mysql"]["host"],user = data_ok["mysql"]["user"],passwd = data_ok["mysql"]["passwd"], 
    #ssl_key = data_ok["mysql"]["ssl_key"],ssl_cert = data_ok["mysql"]["ssl_cert"],ssl_ca = data_ok["mysql"]["ssl_ca"],db = data_ok["mysql"]["db"])
count = 0
#file = open(data["file"]["file_name"],'r')
#f = file.readlines()
def public(lst):
    global count
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

#the_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
rows = []
fields=[]
thread_list = []
w = []
lines = []
stuff = ""
arr = ""
yipee = []
x = 8
q = 0
cwd = os.getcwd()
if str(path.isdir(file_read_path)) == "True":
    os.chdir(file_read_path)
    while True:
        if len(os.listdir())!=0:
            for i in os.listdir():
                start_time = datetime.datetime.now()
                try:
                    with open(i,"r") as data_file:
                        for line in data_file:
                            lines.append(line)
                            if len(lines)==int(data["project"]["maximum_elements"]):
                                public(lines)
                                lines = []
                        if lines:
                            public(lines)
                            lines = []
                except PermissionError as e:
                    continue
                data_file.close()
                os.replace(file_read_path + "\\" + i, files_read + "\\" + i)
                a = "publish"
                s = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values ('" + a + "' ,'" + str(topic_id) + "', '" + str(start_time) + "', '" + str(datetime.datetime.now()) + "', " + str(count) + ")"
                w.append(s)
                count = 0
            if w:
                os.chdir(str(cwd))
                publishing.func1(w)
            w = []
            os.chdir(file_read_path)