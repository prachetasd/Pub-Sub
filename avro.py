import avro.schema
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import io
import datetime
import numpy as np
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding
import threading
import time
#import category_encoders as ce
import requests
from google.auth import crypt
from google.auth import jwt
import os
import os.path
from os import path
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import base64
with open("lright.json") as f1:
    data = json.load(f1)
# TODO(developer): Replace these variables before running the sample.
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
avsc_file = data["project"]["avsc_file"]
credential_path = data["project"]["credentials"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"], 
    ssl_key = data["mysql"]["ssl_key"],ssl_cert = data["mysql"]["ssl_cert"],ssl_ca = data["mysql"]["ssl_ca"],db = data["mysql"]["db"])
#dats = open("lright.json", "r").readlines()
#for i in dats:
    #print(i)
count = 0
q = 0
poi = ""
#start_t = datetime.datetime.now()

def public(k,encoding):
    global count
    for j in k:
        try:
            # Get the topic encoding type.
            if encoding == encoding.JSON:
                #data = json.dumps(j).encode("utf-8")
                poi = json.dumps(json.loads(j.replace("'",'"'))).encode("utf-8")
                #print(f"Preparing a JSON-encoded message:\n{data}")
            else:
                print(f"No encoding specified in {topic_path}. Abort.")
                exit(0)
            future = publisher_client.publish(topic_path, poi)
            count = count + 1
        except:
            #print("reached here")
            #with open ('demofile3.txt', 'w') as writer:
                #writer.write("for topic "+ str(topic_id) + " the message " + str(poi))
            print(f"{topic_id} not found.")

publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)
# Prepare to write Avro records to the binary output stream.
avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
writer = DatumWriter(avro_schema)
bout = io.BytesIO()

# Prepare some data using a Python dictionary that matches t
f = open("demo.txt","a")
info_dat = data["file"]["file_name"]
with open(info_dat, "r") as data:
    info = data.read()
#print(info)
arr = info.split("\n")
eight_split = np.array_split(arr,2)
print(len(arr))
topic = publisher_client.get_topic(request={"topic": topic_path})
encoding = topic.schema_settings.encoding
the_path = "D:\\DoNotDelete\\Documents\\pub_files"
if str(path.isdir(the_path)) == "True":
    os.chdir(the_path)
    #print(len(os.listdir()))
    #mydb = mysql.connector.connect(host=dats["mysql"]["host"],user = dats["mysql"]["user"],passwd = dats["mysql"]["passwd"],ssl_key = dats["mysql"]["ssl_key"],ssl_cert = dats["mysql"]["ssl_cert"],ssl_ca = dats["mysql"]["ssl_ca"],db = dats["mysql"]["db"])
    while True:
        #start_t = datetime.datetime.now()
        os.chdir(the_path)
        if len(os.listdir())!=0: 
            for i in os.listdir():
                start = time.time()
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
                for k in two_split:
                    public(k,encoding)
                    time.sleep(10.0)
                data_file.close()
                os.chdir(the_path)
                os.remove(i)
                #if datetime.datetime.now().minute == 0:
                a_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
                os.chdir(a_path)
                #print("in writing")
                cursor = mydb.cursor()
                a = "publish"
                s = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values ('" + a + "' ,'" + str(topic_id) + "', '" + str(start_time) + "', '" + str(datetime.datetime.now()) + "', " + str(count) + ")"
                #print(s)
                cursor.execute(s)
                mydb.commit()
                #mydb.close()
                with open("dem.txt","r") as dats:
                    inc = dats.read()
                if inc=="": 
                    file1 = open("dem.txt","w")
                    file1.write("Topic " + topic_id + ", Company_id:1 ,Property_id:2 ,Time taken:" + str(time.time()-start) 
                    + " seconds      Number of messages " + str(count) + "\n")
                    file1.close()
                else:
                    file1 = open("dem.txt","a")
                    file1.write("Topic " + topic_id + ", Company_id:1 ,Property_id:2 ,Time interval:" + str(time.time()-start) 
                    + " seconds      Number of messages " + str(count) + "\n")
                    file1.close()
                count = 0
#for k in eight_split:
#time.sleep(10.0)
#public(pic,encoding)
print(count)
end_t = datetime.datetime.now()
#with open("pub_telemtery.txt","w") as p:
    #p.write(start_t + " " + end_t + "    " + count)
#t1.join()
print("took ",end_t-start_t)
