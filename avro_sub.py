import avro
from avro.io import BinaryDecoder, DatumReader
from concurrent.futures import TimeoutError
import io
import json
import time
import sys
import threading
from google.cloud import pubsub_v1
from google.cloud.pubsub import SubscriberClient
from sql import sql_exec
import avro.datafile as avdf
import os
import datetime
with open("config_sub_generic.json") as f1:
    data = json.load(f1)
# TODO(developer)
project_id = data["project"]["project_id"]
subscription_id = data["project"]["sub_id"]
avsc_file = data["project"]["avsc_file"]
# Number of seconds the subscriber listens for messages
credential_path = data["project"]["credentials"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
timeout = float(data["project"]["timeout"])
p = 0
lst = []
lst2 = []
columns = ""
new_string = ""
query = ""
length = 0
r_len = 0
while True:
    subscriber = SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    start_point = datetime.datetime.now()
    avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
    crossed = False
    count = 0
    def callback(message: pubsub_v1.subscriber.message.Message)->None:
        encoding = message.attributes.get("googclient_schemaencoding")
        global p
        global data
        global columns
        global new_string
        global query
        global lst
        global lst2
        global length
        try:
            if encoding == "JSON":
                message_data = json.loads(message.data)
                lst.append(tuple(message_data.values()))
                if p == 0:
                    columns = str(tuple(message_data.keys()))
                    columns = columns.replace("'","")
                    irp = "(" + str("%s," * len(message_data.keys()))
                    string_list = list(irp)
                    string_list[len(string_list)-1] = ")"
                    new_string = "".join(string_list)
                    query = "insert into " + data["mysql"]["table"] + " " + columns +" values " + new_string
                    p = 1
                if len(lst)>=int(data["project"]["maximum_elements"]):
                    crossed = True
                    length = length + len(lst)
                    lst2 = []
                    lst2 = lst
                    lst = []
                    #print("Should be 50K or more " + str(len(lst2)))
                    print("Messages subscribed in curent window of " + str(timeout) + " Seconds " + str(len(lst)))
                    t2 = threading.Thread(target=sql_exec,args=(lst2,query,start_point,subscription_id),daemon=True)
                    t2.start()
                
            message.ack()
        except:
            print("message not retrieved")
    
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception occurs first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.



    length = length + len(lst)
    print("Messages subscribed in curent window of " + str(timeout) + " Seconds is " + str(len(lst)))
    print("Total messages Subscribed " + str(length))
    
    if lst and crossed==False:
        # print("should be less than 50K " + str(len(lst)))
        t1 = threading.Thread(target=sql_exec,args=(lst,query,start_point,subscription_id),daemon=True)
        t1.start()
        lst = []