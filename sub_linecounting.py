from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import os
import json
import sqlalchemy
import itertools
import multiprocessing
from sqlalchemy import create_engine
from insert_polygon_linecount_messages import insert_into_database
import urllib.parse

# TODO(developer)
credential_path = "dev_key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = "dn-us-analytics-dev"
sub_id = "linecount_test-sub"
# Number of seconds the subscriber should listen for messages
timeout = 5.0
password = "fu2*eMoMik6s985AvU34gLj!DrlthoHu"
password = urllib.parse.quote_plus(password)

ssl_args = {
    "ssl_ca": "/home/gcpuser_pdeshpande/ssl_deepnorth/server-ca.pem",
    "ssl_cert": "/home/gcpuser_pdeshpande/ssl_deepnorth/client-cert.pem",
    "ssl_key": "/home/gcpuser_pdeshpande/ssl_deepnorth/client-key.pem"
}

engine = create_engine(
        url="mysql+pymysql://{0}:{1}@{2}:{3}".format(
            "root", password, "10.23.0.3", 3306
        ),connect_args = ssl_args,pool_size=1,pool_recycle=600
    )

# Use this once linecounting table is changed with company_id and property_id
#base_query = "insert into linecounting_records (company_id, property_id, line_id, enter_num, exit_num, create_time) values "
base_query = "insert into linecounting_records (line_id, enter_num, exit_num, create_time) values " 

lst_of_messages = []
line_id_or_polygon_id = "line_id"

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
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
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
        #m1 = multiprocessing.Process(target=insert_into_database,args=(lst_of_messages,sql_engine,base_query,line_id_or_polygon_id))
        #m1.start()
        insert_into_database(lst_of_messages, engine, base_query, line_id_or_polygon_id)
        lst_of_messages = []


        