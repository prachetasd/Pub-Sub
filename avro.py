import avro.schema
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import io
import datetime
import numpy as np
import sys
from concurrent import futures
from google.cloud import pubsub_v1
from google import api_core
from google.api_core import exceptions
from google.api_core.exceptions import NotFound
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
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import base64
import threading
import publishing

with open("config_pub_generic.json") as f1:
    data = json.load(f1)
# TODO(developer): Replace these variables before running the sample.
project_id = data["project"]["project_id"]
topic_id = data["project"]["topic_id"]
avsc_file = data["project"]["avsc_file"]
credential_path = data["project"]["credentials"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
file_read_path = data["project"]["Files_to_read"]
files_read = data["project"]["Files_read"]

count = 0
q = 0
poi = ""

def public(k,encoding):
    global count
    for j in k:
        try:
            # Get the topic encoding type.
            if encoding == encoding.JSON:
                poi = json.dumps(json.loads(j.replace("'",'"'))).encode("utf-8")
                future = publisher_client.publish(topic_path, poi)
                count = count + 1
            else:
                print(f"No encoding specified in {topic_path}. Abort.")
                exit(0)
        except:
            exit(0)
            print(f"{topic_id} not found.")

publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)
# Prepare to write Avro records to the binary output stream.
avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
writer = DatumWriter(avro_schema)
bout = io.BytesIO()
cwd = os.getcwd()
w = []
lines = []
topic = publisher_client.get_topic(request={"topic": topic_path})
encoding = topic.schema_settings.encoding
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
                                public(lines,encoding)
                                lines = []
                        if lines:
                            public(lines,encoding)
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
