######## This program's purpose is to read large amount of data and upload it onto out GOOGLE CLOUD PUB/SUB
####### AUTHOR: PRACHETAS DESHPANDE
####### LAST MODIFIED: 11/17/2021 
from asyncio.log import logger
import os
import json
import datetime
import google.cloud
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from os import path
import logging
from sync import publish_logs
from google.cloud import pubsub_v1
from concurrent import futures
from google.cloud.pubsub_v1.types import (
    LimitExceededBehavior,
    PublisherOptions,
    PublishFlowControl,
)

class PublishClient:

    def __init__(self,table_name,matching_run_date):
        """ Init publisher client """ 
        try:
            self.init_done = False
            if not path.exists("/pub_config/matching_publisher_configurations.json"):
                logging.error("Config file doesnt exists!!!")
                return
            with open("/pub_config/matching_publisher_configurations.json") as pub_config_file:
                data = json.load(pub_config_file)
            self.project_id = data["project"][table_name]["project_id"]                          # Google Pub/Sub project_id on GOOGLE CLOUD 
            self.topic_id = data["project"][table_name]["topic_id"]                              # Google Pub/Sub Topic_id on GOOGLE CLOUD
            if table_name != "generic":
                self.avsc_file = data["project"][table_name]["avsc_file"]                        # The avsc file provided
            self.credential_path = data["project"][table_name]["credentials"]                    # The credentials provided by GOOGLE CLOUD for a specific topic
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credential_path                  # Activate the credentials
            self.name_of_table = table_name
            self.missed_message = ""                                                             # Stores the message if it fails to publish
            self.property_id = ""                                                                # Stores the property_id
            self.total_rows = 0                                                                  # Stores the number of messages published
            self.publish_futures = []
            flow_control_settings = PublishFlowControl(
            message_limit=100,  # 100 messages
            byte_limit=10 * 1024 * 1024,  # 10 MiB
            limit_exceeded_behavior=LimitExceededBehavior.BLOCK,)                                # Controls the maximum messages the client can publish
            self.publisher_client = PublisherClient(publisher_options=PublisherOptions(flow_control=flow_control_settings))   # Declare the publisher client with settings to control the maximum flow of messages
            self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)    # Initializes the path to the topic
            self.topic = self.publisher_client.get_topic(request={"topic": self.topic_path})                   # Get the topic path from our GOOGLE CLOUD ACCOUNT
            self.encoding = self.topic.schema_settings.encoding
            logging.basicConfig(filename=f'/logs/publisher_logs_{matching_run_date}.log', encoding='utf-8', level=logging.INFO)     # Declare a log file to keep track of the publisher's progress
            self.init_done = True
        except Exception as e:
            logging.error("invalid credentials")
            logging.error(e, exc_info=True)

    def __callback(self,publish_future: pubsub_v1.publisher.futures.Future):
      message_id = publish_future.result()
      if message_id == None:
        logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(self.property_id) + ' Message: ' + self.missed_message + 'Status: Failed')  
      else:
        self.total_rows = self.total_rows + 1

    # The public function will publish all the messages read
    def __publish_matching_data(self, list_of_rows,prop_id,comp_id):
        #future = None
        for data_row in list_of_rows:
            try:
                # Get the topic encoding type.
                if self.encoding == self.encoding.JSON:                                           # The encoding must be of JSON type
                    self.property_id = prop_id                                                    # Initialize the property_id
                    self.missed_message = str(data_row)             
                    data_row_encoded = data_row.encode("utf-8")
                    try:
                        future = self.publisher_client.publish(self.topic_path, data_row_encoded) # Publish the message
                        future.add_done_callback(self.__callback)                                  # Callback to make sure the messages are publsihed
                        self.publish_futures.append(future)
                    except google.api_core.exceptions as e:                                        # If schema validation fails
                        logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + 'Message: ' + str(data_row) + 'Status: Failed')
                    except UnicodeDecodeError:
                         logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + 'Message: ' + str(data_row) + 'Status: Failed')
                    #print(total_rows)
            except Exception as e:
                logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + ' Message: ' + str(data_row) + 'Status: Failed')
            except NotFound:                                                            # If messages cannot be found the program will give an error
                logging.error(f"{self.topic_id} not found.")
        rows = futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)                # Wait for all meesges to be published
        self.publish_futures = []
        logging.info(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + ' Messages count: ' + str(self.total_rows) + '; Status: success')
        the_log = "('" + str(datetime.date.today()) + "'," + str(comp_id) + "," + str(prop_id)  + "," + str(len(list_of_rows)) + "," + str(self.total_rows) + ",'matching_records'" + ")"
        publish_logs.initialize_settings(the_log,self.credential_path)                              # Construct and publish the log
        return self.total_rows

    def __run_pub(self,list_of_messages,prop_id,comp_id):
        logging.info(str(datetime.datetime.now()) + ' For property id ' + str(prop_id) + ' Messages Recieved: ' + str(len(list_of_messages)))
        total_rows_of_data = self.__publish_matching_data(list_of_messages,prop_id,comp_id)         # Call the function to publish the list of messages
        list_of_messages = []
        return total_rows_of_data

    def publish_matching_data(self, list_of_messages, prop_id, comp_id):
        if not self.init_done:                                                                      # Check if the constructor is intitalized
            logging.error("Initialization not done!!!")
            return 0
        self.total_rows = 0                                                                         # Set the total messages read to 0
        company_and_property = '{"company_id":"' + comp_id + '","property_id":"' + prop_id + '"'    # Add the company_id and property_id to the message
        self.property_id = prop_id                                                                  # Set the property_id
        list_of_messages = list(map(lambda x:x.replace("{",","),list_of_messages))                  # Construct all the messages
        list_of_messages = list(map(lambda y: company_and_property + y,list_of_messages))
        total_rows_published = self.__run_pub(list_of_messages,prop_id,comp_id)                     # Run the publisher
        return total_rows_published