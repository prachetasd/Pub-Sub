import requests
import time
import os
import io
import json
import sys
from concurrent import futures
from google.cloud import pubsub_v1
from google.cloud.pubsub import PublisherClient
import threading
import base64

def initialize_settings(message,credential_path):
    global publisher
    global topic_path
    if not os.path.exists("/pub_config/pubsub_loggs.json"):                 # find out if the given path to the file exists
        print("path does not exist")                                        # print this if opening the file fails
        return
    with open("/pub_config/pubsub_loggs.json") as f1:                       # open up the configuration file           
        inf = json.load(f1)                                                 # load the information in a variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path          # Activate the google credentials
    project_id = inf['project']['project_id']                               # Initialize the project id
    topic_id = inf['project']['topic_id']                                   # Initialize the topic id
    publisher = pubsub_v1.PublisherClient()                                 # Declare a publisher client
    topic_path = publisher.topic_path(project_id, topic_id)                 # Initialize the entire topic path
    publish_log(message,publisher,topic_path)                                                    # Call the function to publish the log message


def publish_log(message,publisher,topic_path):
    data = f"{message}"                                                     # Set the message as a string
    # Data must be a bytestring
    data = data.encode("utf-8")                                             # Encode the data into a bytestring
    # When you publish a message, the client returns a future.
    try:
        future = publisher.publish(topic_path, data)                        # Publish the given log message
        if future.result()!=None:                                           # If google cloud returns a valid message ID
            print("published")                                              # Print published for the user to know that the operation was successfull
    except:
        print('Error in publishing')                                        # Print this error if message id was not returned
    
