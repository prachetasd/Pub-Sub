import avro.schema
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import io
import numpy as np
from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
from google.pubsub_v1.types import Encoding
import threading
import category_encoders as ce

# TODO(developer): Replace these variables before running the sample.
project_id = "dn-us-analytics-dev"
topic_id = "avro_trial"
avsc_file = "C:\\Users\\prachetas.deshpande\\pub_sub_try\\ex.avsc"
count = 0
def public(lst,encoding):
    for j in lst:
    #print(j)
        try:
            # Get the topic encoding type.
            if encoding == encoding.JSON:
                #data = json.dumps(j).encode("utf-8")
                info = json.dumps(json.loads(j.replace("'",'"'))).encode("utf-8")
                #print(f"Preparing a JSON-encoded message:\n{data}")
            else:
                print(f"No encoding specified in {topic_path}. Abort.")
                exit(0)
            future = publisher_client.publish(topic_path, info)
            #print(f"Published message ID: {future.result()}")
        except NotFound:
            print(f"{topic_id} not found.")
publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)

# Prepare to write Avro records to the binary output stream.
avro_schema = avro.schema.parse(open(avsc_file, "rb").read())
writer = DatumWriter(avro_schema)
bout = io.BytesIO()

# Prepare some data using a Python dictionary that matches t

with open("ex.txt", "r") as data:
        info = data.read()
arr = info.split("\n")
eight_split = np.array_split(arr,8)
print(len(arr))
topic = publisher_client.get_topic(request={"topic": topic_path})
encoding = topic.schema_settings.encoding
for k in arr:
    #t1 = threading.Thread(target = public,args=(k,encoding))
    #t1.start()
    #print(f"Published message ID: {future.result()}")
    if encoding == encoding.JSON:
        #data = json.dumps(j).encode("utf-8")
        info = json.dumps(json.loads(k.replace("'",'"'))).encode("utf-8")
        #print(f"Preparing a JSON-encoded message:\n{data}")
    else:
        print(f"No encoding specified in {topic_path}. Abort.")
        exit(0)
    future = publisher_client.publish(topic_path, info)
    #print(count)
    count = count + 1
print(count)