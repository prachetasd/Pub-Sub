from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time
import ast
import os
import json
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import threading
from sql import sql_exec
#from run import mydb
# TODO(developer)
credential_path = "the_new.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = "dn-us-analytics-dev"
subscription_id = "sensors-sub"
length = 0
# Number of seconds the subscriber should listen for messages
timeout = 30.0
p = 0
query = ""
columns = ""
mydb=mysql.connector.connect(host='localhost',user='root',password='Genius_1998',database='db_prod')
while True:
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    lst =[]
    pic = []
    #print("In sub.py ",mydb)
    def callback(message):
        global p
        global query
        global columns
        info = message.data.decode("utf-8")
        #print(info)
        info = info.replace("'",'"')
        info = "'"+info+"'"
        info = info.replace('"','',1)
        irp = list(info)
        irp[len(info)-2]=""
        new_string = "".join(irp)
        new_string = new_string[:2] + '"' + new_string[2:]
        new_string = new_string.replace("'","") + "}"
        res = json.loads(new_string)
        if p ==0:
            ice = res.keys()
            columns = str(tuple(res.keys()))
            columns = columns.replace("'","")
            ice = "(" + str("%s," * len(res.keys()))
            string_list = list(ice)
            string_list[len(string_list)-1] = ")"
            new = "".join(string_list)
            query = "insert into matching_records " + columns +" values " + new
            p = 1
        lst.append(res.values())
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
        t1 = threading.Thread(target = sql_exec,args=(lst,query))
        t1.start()
        #p = p + 1
    #elif lst:
        #t2 = threading.Thread(target = sql_exec,args=(lst,mydb))
        #t2.start()