######## This program's purpose is to read large amount of data and upload it onto out GOOGLE CLOUD PUB/SUB
####### AUTHOR: PRACHETAS DESHPANDE
####### LAST MODIFIED: 11/17/2021 
from asyncio.log import logger
import os
import json
import datetime
import shutil
import google.cloud
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from os import path
import avro_validator
from avro_validator.schema import Schema
import logging
import publishing

#logging.basicConfig(filename='publisher_logs.log', encoding='utf-8', level=logging.INFO)

class PublishClient:

    def __init__(self,table_name):
        """ Init publisher client """ 
        try:
            self.init_done = False
            if not path.exists("/pub_config/config_pub_avro_schema.json"):
                logging.error("Config file doesnt exists!!!")
                return
            with open("config_pub_avro_schema.json") as pub_config_file:
                data = json.load(pub_config_file)
            self.project_id = data["project"][table_name]["project_id"]                          # Google Pub/Sub project_id on GOOGLE CLOUD 
            self.topic_id = data["project"][table_name]["topic_id"]                              # Google Pub/Sub Topic_id on GOOGLE CLOUD
            if table_name != "generic":
                self.avsc_file = data["project"][table_name]["avsc_file"]                            # The avsc file provided
            self.credential_path = data["project"][table_name]["credentials"]                    # The credentials provided by GOOGLE CLOUD for a specific topic
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credential_path
            self.name_of_table = table_name
            self.publisher_client = PublisherClient()                   # The avsc file that is used for schema validation
            self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
            self.topic = self.publisher_client.get_topic(request={"topic": self.topic_path})                   # Get the topic path from our GOOGLE CLOUD ACCOUNT
            self.encoding = self.topic.schema_settings.encoding
            logging.basicConfig(filename=f'/log/publisher_logs_{matching_run_date}.log', encoding='utf-8', level=logging.INFO)
            #shutil.copy(f'/dnhq-filer1/archived_vids/INGKA/{matching_run_date}.mp4',f'/log/matching_pub')
            self.init_done = True
        except Exception as e:
            logging.error("invalid credentials")
            logging.error(e, exc_info=True)

    # The public function will publish all the messages read
    def __publish_data(self, list_of_rows,prop_id,comp_id):
        total_rows = 0
        #future = None
        for data_row in list_of_rows:
            try:
                # Get the topic encoding type.
                if self.encoding == self.encoding.JSON:                                           # The encoding must be of JSON type
                    data_row_encoded = data_row.encode("utf-8")
                    try:
                        future = self.publisher_client.publish(self.topic_path, data_row_encoded)
                    except google.api_core.exceptions as e:
                        logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + 'Message: ' + str(data_row) + 'Status: Failed')
                    except UnicodeDecodeError:
                         logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + 'Message: ' + str(data_row) + 'Status: Failed')
                    if future.result() == None:
                        logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + ' Message: ' + str(data_row) + 'Status: Failed')  
                    else:
                        total_rows = total_rows + 1
                    #print(total_rows)
            except Exception as e:
                logging.error(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + ' Message: ' + str(data_row) + 'Status: Failed')
                logging.error(e, exc_info=True)
            #else:
                #logging.error(f"No encoding specified in {self.topic_path}. Abort.")
            except NotFound:                                                            # If messages cannot be found the program will give an error
                logging.error(f"{self.topic_id} not found.")
        logging.info(str(datetime.datetime.now()) + ' Topic_id: ' + self.topic_id + '; Property_id ' + str(prop_id) + ' Messages count: ' + str(total_rows) + '; Status: success')
        the_log = "('" + str(datetime.date.today()) + "'," + str(comp_id) + "," + str(prop_id)  + "," + str(len(list_of_rows)) + "," + str(total_rows) + ",'matching_records'" + ")"
        #print(the_log)
        publishing.func1(the_log)
        return total_rows

    def __run_pub(self,lines,prop_id,comp_id):
        logging.info(str(datetime.datetime.now()) + ' For property id ' + str(prop_id) + ' Messages Recieved: ' + str(len(lines)))
        total_rows_of_data = self.__publish_data(lines,prop_id,comp_id)
        lines = []
        return total_rows_of_data

    def publish_data(self, lines, prop_id, comp_id):
        #lines = []
        if not self.init_done:
            logging.error("Initialization not done!!!")
            return 0
        company_and_property = '{"company_id":"' + comp_id + '","property_id":"' + prop_id + '"'
        lines = list(map(lambda x:x.replace("{",","),lines))
        lines = list(map(lambda y: company_and_property + y,lines))
        total_rows_published = self.__run_pub(lines,prop_id,comp_id)
        return total_rows_published