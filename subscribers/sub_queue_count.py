from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os
import json
import sqlalchemy
import itertools
from sqlalchemy import create_engine
import multiprocessing
from insert_polygon_linecount_messages import insert_into_tables
# TODO(developer)
with open("queue_count_config.json") as f1:
    inf = json.load(f1)
credential_path = inf['project']['credentials']
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = inf['project']['project_id']
sub_id = inf['project']['sub_id']
# Number of seconds the subscriber should listen for messages
timeout = float(inf['project']['timeout'])
sql_engine = create_engine("mysql+pymysql://{user}:{pw}@{hostname}"
                        .format(user=inf["mysql"]["user"],
                                hostname=inf["mysql"]["host"],
                                pw=inf["mysql"]["passwd"]),pool_recycle=inf["project"]["pool_recycle"])
base_query = "insert into queuecount_records (company_id,property_id,polygon_id,timestamp,counting,create_time) values "

lst_of_messages = []
line_id_or_polygon_id = "polygon_id"

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    encoding = message.attributes.get("googclient_schemaencoding")
    #print(f"Received {message}.")
    message.ack()
    try:
        if encoding == "JSON":
            message_data = json.loads(message.data)                         # Load the data into a variable
            lst_of_messages.append(tuple(message_data.values()))
    except:
        print("message not retrieved")

while True:
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, sub_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback,flow_control=50000)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

    if lst_of_messages:
        m1 = multiprocessing.Process(target=insert_into_tables,args=(lst_of_messages,sql_engine,base_query,line_id_or_polygon_id))
        m1.start()
        #insert_into_tables(lst_of_messages, engine, base_query, line_id_or_polygon_id)
        lst_of_messages = []


        