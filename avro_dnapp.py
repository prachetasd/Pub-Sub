######## This program's purpose is to read large amount of data and upload it onto out GOOGLE CLOUD PUB/SUB
####### AUTHOR: PRACHETAS DESHPANDE
####### LAST MODIFIED: 11/17/2021 
#import avro
import avro.schema
#from avro.datafile import DataFileReader, DataFileWriter
#from avro.io import DatumReader, DatumWriter
import json
import io
import datetime
#import numpy as np
import sys
import schedule
from concurrent import futures
import google.cloud
from google import api_core
from google.api_core import exceptions
from google.api_core.exceptions import NotFound, Aborted
from google.api_core.exceptions import AlreadyExists, InvalidArgument
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding
import threading
import time
import grpc
import requests
from google.auth import crypt
from google.auth import jwt
import os
import os.path
from os import path
#import mysql.connector
#from mysql.connector.errors import OperationalError, ProgrammingError
import base64
import threading
import publishing
import pickle

with open(sys.argv[len(sys.argv)-1]) as f1:
    data = json.load(f1)
# TODO(developer): Replace these variables before running the sample.
project_id = data["project"]["project_id"]                          # Google Pub/Sub project_id on GOOGLE CLOUD 
topic_id = data["project"]["topic_id"]                              # Google Pub/Sub Topic_id on GOOGLE CLOUD
avsc_file = data["project"]["avsc_file"]                            # The avsc file provided
credential_path = data["project"]["credentials"]                    # The credentials provided by GOOGLE CLOUD for a specific topic
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path      
file_read_path = data["project"]["Files_to_read"]                   # The folder from where the messages will be read
files_read = data["project"]["Files_read"]                          # The folder that will store all the files whose messages are published

#count = 0
total_files_read = 0
q = 0
file_number = 0
logs = []
total_len = 0 
poi = ""
total = 0
total_rows = 0
f2 = open("publish_log_file.txt","a")
#cur = datetime.datetime.now().minute
# The public function will publish all the messages read
def public(k,encoding):
    #count = 0
    #global w
    global total_rows
    #global cur
    for j in k:
        try:
            # Get the topic encoding type.
            if encoding == encoding.JSON:                                           # The encoding must be of JSON type
                poi = json.dumps(json.loads(j.replace("'",'"'))).encode("utf-8")
                #f2.write(str(poi) + "\n")
                future = publisher_client.publish(topic_path, poi)
                if future.result() == None:
                        print(j)
                else:
                        total_rows = total_rows + 1
                #count = count + 1
            else:
                print(f"No encoding specified in {topic_path}. Abort.")
                exit(0)
        except NotFound:                                                            # If messages cannot be found the program will give an error
            print(f"{topic_id} not found.")
    #query = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values ('publish', '" + str(topic_id) + "', '" + str(start_time) + "', '"+ str(datetime.datetime.now()) + "', " + str(count) + ")"
    #w.append(query) 

def amt():
	global total_rows
        #global topic_id
	#query = "insert into pub_logs (Date,hour,published_messages,Property_id,Topic_name) values (%s,%s,%s,%s,%s)"
	a = "1"
	log = "insert into pub_logs (Date,hour,published_messages,Property_id,Topic_name) values ('" + str(datetime.date.today()) + "','" + str(datetime.datetime.now().hour-1) + "','"+ str(total_rows) + "','" + a + "','" + topic_id + "')"
        #logs.append(log)
	publishing.func1(log)
	total_rows = 0

def scheduler():
	schedule.every().hour.do(amt)
	while True:
		schedule.run_pending()

t= threading.Thread(target = scheduler,args=())
t.start()

publisher_client = PublisherClient()
#publisher_client = pubsub_v1.PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)
# Prepare to write Avro records to the binary output stream.
avro_schema = avro.schema.parse(open(avsc_file, "rb").read())                       # The avsc file that is used for schema validation
#writer = DatumWriter(avro_schema)
#bout = io.BytesIO()
cwd = os.getcwd()
total_files = 0
#w = []
lines = []
#data_file = []
topic = publisher_client.get_topic(request={"topic": topic_path})                   # Get the topic path from our GOOGLE CLOUD ACCOUNT
encoding = topic.schema_settings.encoding
if str(path.isdir(file_read_path)) == "True":
    #os.chdir(file_read_path)
    while True:
        if len(os.listdir(file_read_path))!=0:
            for i in os.listdir(file_read_path):                                                      # The loop will give the name of all files that need to publish
                #start_time = datetime.datetime.now()
                try:
                    with open(file_read_path + "/" + i,"r") as data_file:
				#data_file = list(f)
                        #total_len = total_len + len(data_file)
                        #try:
                         #data_file = pickle.load(pickle_off)
                        #except:
                         #continue
                        #data_file = list(f)
                       # print(len(data_file))
                        f2.write(str(file_read_path + "/" + i))
                        for line in data_file:
                            lines.append(line)
                            if len(lines)==int(data["project"]["maximum_elements"]):    # If the list exceeds a certain limit it will publish
                                total = total + len(lines)
                                public(lines,encoding)
                                #t1 = threading.Thread(target=public,args=(lines,encoding,start_time))
                                #t1.start()
                                lines = []
                        if lines:
                            total = total + len(lines)
                            public(lines,encoding)  # If the length of the list stays below the limit
                            lines = []
                        total_files = total_files + 1         
                except PermissionError as e:
                    continue
                except FileNotFoundError as e:
                    continue
                data_file.close()
                os.replace(file_read_path + "/" + i, files_read + "/" + i)
