from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time
import ast
import os
import json
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import threading
import datetime
from sql import sql_exec
#from run import mydb
# TODO(developer)
with open("lright.json") as f1:
    data = json.load(f1)
credential_path = data["project"]["credentials"]
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = data["project"]["project_id"]
subscription_id = data["project"]["sub_id"]
length = 0
# Number of seconds the subscriber should listen for messages
timeout = float(data["project"]["timeout"])
p = 0
query = ""
columns = ""
#mydb=mysql.connector.connect(host='localhost',user='root',password='Genius_1998',database='db_prod')
while True:
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    start_point = datetime.datetime.now()
    lst =[]
    pic = []
    #print("In sub.py ",mydb)
    def callback(message):
        global p
        global query
        global columns
        info = message.data.decode("utf-8")
        info = info.replace("'",'"')
        #print(info)
        #lst.append(info)
        res = json.loads(info)
        #print(res)
        if p ==0:
            ice = res.keys()
            columns = str(tuple(res.keys()))
            columns = columns.replace("'","")
            ice = "(" + str("%s," * len(res.keys()))
            string_list = list(ice)
            string_list[len(string_list)-1] = ")"
            new = "".join(string_list)
            query = "insert into " + data["mysql"]["table"] + " " + columns +" values " + new
            p = 1
        lst.append(res.values())
        #print(lst)
        message.ack()


    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    start=time.time()
    with subscriber:
        try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    #print("finished next set of messages")
    length = length + len(lst)
    print(length)
    if lst:
        #sql_exec(lst,mydb)
        #if lst and t1.is_alive()=="False":
        t1 = threading.Thread(target = sql_exec,args=(lst,query,start_point,subscription_id),daemon=True)
        t1.start()
        #p = p + 1
    #elif lst:
        #t2 = threading.Thread(target = sql_exec,args=(lst,mydb))
        #t2.start()