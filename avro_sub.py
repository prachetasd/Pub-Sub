import avro
from avro.io import BinaryDecoder, DatumReader
from concurrent.futures import TimeoutError
import io
import json
import threading
from google.cloud import pubsub_v1
from google.cloud.pubsub import SubscriberClient
from sql import sql_exec
import avro.datafile as avdf
import os
import datetime
with open("lright.json") as f1:
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
columns = ""
new_string = ""
query = ""
length = 0
while True:
    subscriber = SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    start_point = datetime.datetime.now()
    avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
    lst = []
    def callback(message: pubsub_v1.subscriber.message.Message)->None:
        encoding = message.attributes.get("googclient_schemaencoding")
        global p
        global columns
        global new_string
        global query
        try:
            if encoding == "JSON":
                message_data = json.loads(message.data)
                #print(message.data)
                if p == 0:
                    lst.append(tuple(message_data.values()))
                    columns = str(tuple(message_data.keys()))
                    columns = columns.replace("'","")
                    irp = "(" + str("%s," * len(message_data.keys()))
                    string_list = list(irp)
                    string_list[len(string_list)-1] = ")"
                    new_string = "".join(string_list)
                    query = "insert into " + data["mysql"]["table"] + " " + columns +" values " + new_string
                    p = 1
                else:
                    lst.append(tuple(message_data.values()))
            message.ack()
        except:
            print("message not retrieved")
    #flow_control = pubsub_v1.types.FlowControl(max_messages=100)
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
    print(length)
    if lst:
        t1 = threading.Thread(target=sql_exec,args=(lst,query,start_point,subscription_id),daemon=True)
        t1.start()
        #sql_exec(lst,query)