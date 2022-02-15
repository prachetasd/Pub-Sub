######## This program's purpose is to read large amount of data and upload it onto out GOOGLE CLOUD PUB/SUB
####### AUTHOR: PRACHETAS DESHPANDE
####### LAST MODIFIED: 11/17/2021 
#import avro
import os
from os import path
import shutil
import distutils
import json
import time
from distutils.dir_util import copy_tree
import pickle
import avro.schema
import io
import datetime
import sys
import schedule
from concurrent import futures
import google.cloud
from google import api_core
from google.api_core import exceptions
from google.api_core.exceptions import NotFound, Aborted
from google.api_core.exceptions import AlreadyExists, InvalidArgument
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding
import threading
import os.path
import logging
import publishing

class pub_data:
    # The public function will publish all the messages read
    def public(list_of_rows,encoding,logger,handler,total_rows,publisher_client,topic_path):
        #global total_rows
        for j in list_of_rows:
            try:
                # Get the topic encoding type.
                if encoding == encoding.JSON:                                           # The encoding must be of JSON type
                    the_message = json.dumps(json.loads(j)).encode("utf-8")
                    future = publisher_client.publish(topic_path, the_message)
                    #print("published")
                    if future.result() == None:
                        logger.addHandler(handler)
                        logger.error(str(datetime.datetime.now()) + ' Message failed to publish is :' + str(j))  
                    else:
                        total_rows = total_rows + 1
                else:
                    logger.error(f"No encoding specified in {topic_path}. Abort.")
                    exit(0)
            except NotFound:                                                            # If messages cannot be found the program will give an error
                print(f"{topic_id} not found.")

    def amt():
        global total_rows
        global data
        the_hour = ""
        the_date = ""
        if str(datetime.datetime.now().hour) == "0":
            the_hour = 23
            the_date = str(datetime.datetime.today() - datetime.timedelta(days=1))
        else:
            the_hour = datetime.datetime.now().hour - 1
            the_date = datetime.datetime.today()
        a = "1"
        log = "'" + str(the_date) + "','" + str(the_hour) + "','"+ str(total_rows) + "','" + a + "','" + topic_id + "'"
        publishing.func1(log,data)
        total_rows = 0

    def run_pub(lines,encoding,logger,handler,total_rows,publisher_client,topic_path):
        #schedule.every().hour.do(amt)
        #lines = []
        #while True:
        lines = list(map(lambda x:x.replace("'",'"'),lines))
        #print("Inside run_pub before public")
        pub_data.public(lines,encoding,logger,handler,total_rows,publisher_client,topic_path)
        lines = []

    def main(lines,s,project_id,topic_id,avsc_file):
        #with open("config_pub_avro_schema.json") as f1:
            #data = json.load(f1)
        #project_id = data["project"][s]["project_id"]                          # Google Pub/Sub project_id on GOOGLE CLOUD 
        #topic_id = data["project"][s]["topic_id"]                              # Google Pub/Sub Topic_id on GOOGLE CLOUD
        #avsc_file = data["project"][s]["avsc_file"]                            # The avsc file provided
        #credential_path = data["project"][s]["credentials"]                    # The credentials provided by GOOGLE CLOUD for a specific topic
        #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path      
        #file_read_path = data["project"][s]["Files_to_read"]                   # The folder from where the messages will be read
        #files_read = data["project"]["linecounting"]["Files_read"]                          # The folder that will store all the files whose messages are published
        file_number = 0
        logs = [] 
        total_rows = 0
        the_message = ""
        logger = logging.getLogger()
        handler = logging.FileHandler('logfile.log')
        publisher_client = PublisherClient()
        topic_path = publisher_client.topic_path(project_id, topic_id)
        # Prepare to write Avro records to the binary output stream.
        avro_schema = avro.schema.parse(open(avsc_file, "rb").read())                       # The avsc file that is used for schema validation
        #lines = []
        topic = publisher_client.get_topic(request={"topic": topic_path})                   # Get the topic path from our GOOGLE CLOUD ACCOUNT
        encoding = topic.schema_settings.encoding
        #print("before run_pub")
        pub_data.run_pub(lines,encoding,logger,handler,total_rows,publisher_client,topic_path)


  
  
