from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import avro
from avro.io import BinaryDecoder, DatumReader
from concurrent.futures import TimeoutError
import io
import json
import time
import sys
import threading
import mysql.connector
#import google
from google.cloud import pubsub_v1
from google.cloud.pubsub import SubscriberClient
import sqlalchemy
import mysql
import pymysql
from sqlalchemy import create_engine
from insert_data import insert_all_data
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError
#from pub_data_insert import insert_all_data
import avro.datafile as avdf
import pandas as pd
import os
from multiprocessing import Process
import urllib.parse
import datetime
import pickle
import schedule
from mysql.connector.errors import OperationalError, ProgrammingError, InterfaceError, IntegrityError
import uuid
import google
import collections
from collections import Counter
from cryptography.fernet import Fernet
# TODO(developer)
config_file = "config_" + sys.argv[1] + ".json"
with open(config_file, 'rb') as enc_file:
        decrypt_data = enc_file.read()

with open('subscribe.key', 'rb') as filekey:
   f1 = filekey.read()
f = Fernet(f1)
decrypted = f.decrypt(decrypt_data)
info = decrypted.decode()
data = json.loads(info)

project_id = data["project"]["project_id"]
subscription_id = data["project"]["sub_id"]
# Number of seconds the subscriber should listen for messages
timeout = float(data["project"]["timeout"])
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = data["project"]["credentials"]

columns = ["company_id","property_id","zone_id","camera_id","line_id","is_enter","video_id","track_id","object_id","zone_object_id","create_time"]
df = pd.DataFrame()
avro_schema = avro.schema.parse(open("ex.avsc", "rb").read())               # The avsc file used for schema validation
num_of_messages = 0
logs = ""
lst = []
password = "!8K5yadREVU8!Stu#E?oV7nEfU?ad+Ub"
passwd = urllib.parse.quote_plus(password)

ssl_args = {
    "ssl_ca": "/home/gcpuser_pdeshpande/ssl_europe/server-ca.pem",
    "ssl_cert": "/home/gcpuser_pdeshpande/ssl_europe/client-cert.pem",
    "ssl_key": "/home/gcpuser_pdeshpande/ssl_europe/client-key.pem"
}

#engine = create_engine(
        #url="mysql+pymysql://{0}:{1}@{2}:{3}".format(
            #"root", passwd, "35.246.0.57", 3306
        #),connect_args = ssl_args,pool_recycle=400
    #)

engine2 = create_engine("mysql+pymysql://{user}:{pw}@{hostname}"
                        .format(user=data["mysql"]["7"]["user"],
                                hostname=data["mysql"]["7"]["host"],
                                pw=data["mysql"]["7"]["passwd"]),pool_recycle=300)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    encoding = message.attributes.get("googclient_schemaencoding")
    message.ack()
    global data
    global engine2
    global lst
    global message_data
    try:
        if encoding == "JSON":
            message_data = json.loads(message.data)                         # Load the data into a variable
            #df = df.append(message_data,ignore_index=True)
            lst.append(message_data)
            #if len(lst) == maximum_elements:
                #df2 = pd.DataFrame(lst)
                #t2 = Process(target = insert_all_data,args=(df2,data,engine2))
                #t2.start()
                #lst = []
                #df2 = df2[0:0]
                
    except:
        print("message not retrieved")

while True:
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    #print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
        except google.api_core.exceptions.ResourceExhausted as e:
            streaming_pull_future.cancel()
        except AttributeError as e:
            streaming_pull_future.cancel()
        except Exception as e:
            streaming_pull_future.cancel()
    #print("total messages are " + str(len(lst)))
    if lst:
        #lst = list(dict.fromkeys(lst))
        df = pd.DataFrame(lst,columns = columns)
        df.drop_duplicates(keep=False,inplace=True)
        #print(df)
        #print("len of dataframe is " + str(len(df)))
        t1 = Process(target=insert_all_data,args=(df,data,engine2))
        t1.start()
        num_of_messages = 0
        lst = []
        df = df[0:0]
