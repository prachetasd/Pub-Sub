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
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
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
mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"],
ssl_key = data["mysql"]["ssl_key"],ssl_cert = data["mysql"]["ssl_cert"],ssl_ca = data["mysql"]["ssl_ca"],db = data["mysql"]["db"])
p = 0
q = 0
lst = []
lst2 = []
columns = ""
new_string = ""
query = ""
length = 0
r_len = 0
old_table = ""
new_table = ""
while True:
    subscriber = SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    start_point = datetime.datetime.now()
    crossed = False
    count = 0
    invalid = {"table_name"}
    def callback(message):
        #encoding = message.attributes.get("googclient_schemaencoding")
        global p
        global data
        global columns
        global new_string
        global query
        global lst
        global lst2
        global length
        global old_table
        #global mydb
        global new_table
        info = message.data.decode("utf-8")
        info = info.replace("'",'"')
        res = json.loads(info)
        #new_table = res['table_name']
        #del res['table_name']
        lst2.append(res)
        message.ack()
        #except:
           # print("message not retrieved")
    
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



    length = length + len(lst2)
    print("Messages subscribed in curent window of " + str(timeout) + " Seconds is " + str(len(lst2)))
    print("Total messages Subscribed " + str(length))

    #def without_keys(d, keys):
    #return {x: d[x] for x in d if x not in keys}
    #without_keys(my_dict, invalid)
    
    if lst2:
        for res in lst2:
            new_table = res['table_name']
            if q ==0 or new_table!=old_table:
                if new_table!=old_table and q!=0:
                    mycursor = mydb.cursor()
                    #print(lst[len(lst)-1])
                    mycursor.executemany(query, lst)
                    mydb.commit()
                    lst = []
                old_table = new_table
                del res['table_name']
                columns = str(tuple(res.keys()))
                columns = columns.replace("'","")
                #print(columns)
                irp = "(" + str("%s," * len(res.keys()))
                string_list = list(irp)
                string_list[len(string_list)-1] = ")"
                new_string = "".join(string_list)
                query = "insert into " + old_table + " " + columns +" values " + new_string
                q = 1
            else:
                del res['table_name']
            lst.append(tuple(res.values()))
            #print("done")
        if lst:
            mycursor = mydb.cursor()
            #print(lst[len(lst)-1])
            mycursor.executemany(query, lst)
            mydb.commit()
            lst = []
        lst2 = []
