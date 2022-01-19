######## This program's purpose is to ingest large amount of data from our GOOGLE CLOUD PUB/SUB and insert that data into the MYSQL database
####### AUTHOR: PRACHETAS DESHPANDE
####### LAST MODIFIED: 11/17/2021 
import avro
from avro.io import BinaryDecoder, DatumReader
from concurrent.futures import TimeoutError
import io
import json
import time
import sys
import threading
import mysql.connector
from google.cloud import pubsub_v1
from google.cloud.pubsub import SubscriberClient
from sql import sql_exec
import avro.datafile as avdf
import os
import datetime
import pickle
import schedule
from mysql.connector.errors import OperationalError, ProgrammingError, InterfaceError, IntegrityError
import uuid
#from google.cloud import firestore

#firestore_client = firestore.Client()

#def is_duplicated_message(message_id):


with open("config_sub_avro_schema.json") as f1:
    data = json.load(f1)
# TODO(developer)
project_id = data["project"]["project_id"]                                      # The project id of Pub/Sub on Google Cloud
subscription_id = data["project"]["sub_id"]                                     # The subscription id of Pub/Sub on Google Cloud
avsc_file = data["project"]["avsc_file"]                                        # The avsc file for the data
# Number of seconds the subscriber listens for messages
credential_path = data["project"]["credentials"]                                # The credentials to subscribe to the messages
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
timeout = float(data["project"]["timeout"])                                     # The time frame given for every subscription pull

def amt():
    #data = json.load(f1)
    global num_of_messages
    mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"],db = data["mysql"]["db"])
    my_hour_cursor = mydb.cursor()
    hour_query = "select hour(now())"
    my_hour_cursor.execute(hour_query)
    the_hour = my_hour_cursor.fetchone()
    #date_query = "select date(now())"
    #my_date_cursor.execute(date_query)
    #the_date = my_date_cursor.fetchone()
    if the_hour == 0:
        the_hour = 23
        query = "insert into sub_logs(Date,hour,subscribed_messages,Property_id,Sub_name) values (date(now())-1" + ",'" + str(the_hour) + "','" + str(num_of_messages) + "',1,'" + subscription_id + "')"
    else:
        query = "insert into sub_logs(Date,hour,subscribed_messages,Property_id,Sub_name) values (date(now())" + "," + "hour(now())-1" + ",'" + str(num_of_messages) + "',1,'" + subscription_id + "')"
    my_cursor = mydb.cursor()
    my_cursor.execute(query)
    mydb.commit()
    
def scheduler():
    schedule.every().hour.do(amt)
    while True:
        schedule.run_pending()
        #time.sleep(1)

p = 0
lst = []
lst2 = []
lst3 = []
columns = ""
new_string = ""
query = ""                                                                      # Strings needed to construct the MYSQL query
old_query = ""
total = 0
s = ""
avro_schema = avro.schema.parse(open(avsc_file, "rb").read())               # The avsc file used for schema validation
crossed = False
length = 0
r = 0
num_of_messages = 0
format = "%Y-%m-%d %H:%M:%S"                                                    # Format of the timestamp
t = threading.Thread(target = scheduler, args = ())
t.start()

def callback(message: pubsub_v1.subscriber.message.Message)->None:
    encoding = message.attributes.get("googclient_schemaencoding")
    message.ack()
    #print(message)
    global p
    global s
    global data
    global columns
    global new_string
    global query
    global old_query
    global lst
    global lst2
    global lst3
    global num_of_messages
    global length
    try:
        if encoding == "JSON":
            message_data = json.loads(message.data)                         # Load the data into a variable
            #print(message["body"])
            #else:
            #if is_duplicated_message(message.message_id):
                #print("ok found duplicate")
            #else:
            lst.append(tuple(message_data.values()))                    # insert the data into a list
            num_of_messages = num_of_messages + 1
            if p == 0:
                columns = str(tuple(message_data.keys()))
                columns = columns.replace("'","")
                irp = "(" + str("%s," * len(message_data.keys()))
                string_list = list(irp)
                string_list[len(string_list)-1] = ")"
                new_string = "".join(string_list)
                query = "insert into " + data["mysql"]["table"] + " " + columns +" values " + new_string    # Construct the MYSQL query
                p = 1
            if len(lst)>=int(data["project"]["maximum_elements"]):
                length = length + len(lst)
                lst2 = []
                lst2 = lst
                lst = []
                #print("Messages subscribed in curent window of " + str(timeout) + " Seconds " + str(len(lst2)))
                t2 = threading.Thread(target=sql_exec,args=(lst2,query,start_point,subscription_id))
                t2.start()                                           # insert the data if the list has exceeded the limit
                lst2 = []
    except:
        print("message not retrieved")

while True:
    subscriber = SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    start_point = datetime.datetime.now()
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    #print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception occurs first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.


    lst = list(dict.fromkeys(lst))
    length = length + len(lst)
    #print("Messages subscribed in curent window of " + str(timeout) + " Seconds is " + str(len(lst)))
    #print("Total messages Subscribed " + str(length))
    
    # If the list is less than the limit
    if lst and crossed==False:
        #with open('data_sub.txt', 'wb') as fh:
            #pickle.dump(lst, fh)
        t1 = threading.Thread(target=sql_exec,args=(lst,query,start_point,subscription_id))
        t1.start()
        t1.join()
        lst = []
        #print(threading.active_count())
