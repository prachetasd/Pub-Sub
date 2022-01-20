import csv
import datetime
# csv file name
from google.cloud import pubsub_v1
import time
from concurrent.futures import TimeoutError
import ast
import os
import json
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import threading
import datetime

with open("pubsub_loggs.json") as f1:
    inf = json.load(f1)
credential_path = inf['project']['credentials']
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = inf['project']['project_id']
subscription_id = inf['project']['sub_id']
timeout = float(inf['project']['timeout'])
lst = []
#with open("config_sub_generic.json") as f1:
    #data = json.load(f1)
query = "insert into pub_log (Date,hour,published_messages,Property_id,Topic_name) values (%s,%s,%s,%s,%s)"
def insertion(lst):
    global inf
    global query
    mydb = mysql.connector.connect(host=inf["mysql"]["host"],user = inf["mysql"]["user"],passwd = inf["mysql"]["passwd"],db = inf["mysql"]["db"])
        #s = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values (" + str(new)
    cursor = mydb.cursor()
    try:
        cursor.executemany(query,lst)
        mydb.commit()
    except ProgrammingError as e:
        print("whoopsy")
    mydb.close()
        #lst = []

def callback(message):
    global lst
    #global data
    #mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"],ssl_key = data["mysql"]["ssl_key"],ssl_cert = data["mysql"]["ssl_cert"],ssl_ca = data["mysql"]["ssl_ca"],db = data["mysql"]["db"])
    info = message.data.decode("utf-8")
    lst.append(info)
    message.ack()


while True:
    subscriber = pubsub_v1.SubscriberClient()
        # The `subscription_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    #start=time.time()
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

    print(len(lst))
    #lst = []
    if lst:
        t1 = threading.Thread(target=insertion,args=[lst],daemon=True)
        t1.start()
        t1.join()
        lst = []

    
