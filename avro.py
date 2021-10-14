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
import category_encoders as ce
import requests
from google.auth import crypt
from google.auth import jwt
import os
import base64
with open("lright.json") as f1:
    data = json.load(f1)
# TODO(developer): Replace these variables before running the sample.
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
avsc_file = data["project"]["avsc_file"]
credential_path = data["project"]["credentials"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
count = 0
q = 0
poi = ""
start_t = datetime.datetime.now()

publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)
# Prepare to write Avro records to the binary output stream.
avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
writer = DatumWriter(avro_schema)
bout = io.BytesIO()

# Prepare some data using a Python dictionary that matches t
f = open("demo.txt","a")
info_dat = data["file"]["file_name"]
old_file = info_dat 
with open(info_dat, "r") as data:
    info = data.read()
#print(info)
arr = info.split("\n")
eight_split = np.array_split(arr,2)
print(len(arr))
topic = publisher_client.get_topic(request={"topic": topic_path})
encoding = topic.schema_settings.encoding
for k in eight_split:
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
    time.sleep(10.0)
#public(pic,encoding)
print(count)
end_t = datetime.datetime.now()
#with open("pub_telemtery.txt","w") as p:
    #p.write(start_t + " " + end_t + "    " + count)
#t1.join()
print("took ",end_t-start_t)
