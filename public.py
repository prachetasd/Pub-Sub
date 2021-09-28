import requests
from google.auth import crypt
from google.auth import jwt
import time
import os
import io
import json
import base64
from google.pubsub_v1.types import Encoding

# Service account key path
credential_path = "the_new.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


sa_keyfile = credential_path


# Get service account email and load the json data from the service account key file. 
with io.open(credential_path, "r", encoding="utf-8") as json_file:
    data = json.loads(json_file.read())
    sa_email=data['client_email']


# Audience 
audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"


# TOPIC -  REST End Point 
# "https://pubsub.googleapis.com/v1/projects/test-project/topics/topic-1:publish"
url = "https://pubsub.googleapis.com/v1/projects/dn-us-analytics-dev/topics/sensors:publish"


# Message data
json_payload = [
        {
            "id": "1",
            "price": "$1.99",
            "product": "shirt"


        },
        {
            "id": "3",
            "price": "$3.99",
            "product": "shirt"


        },
        {
            "id": "4",
            "price": "$4.99",
            "product": "shirt"


        },
        {
            "id": "4",
            "price": "$4.99",
            "product": "shirt"


        },
        {
            "id": "6",
            "price": "$6.99",
            "product": "shirt"


        },
        {
            "id": "7",
            "price": "$7.99",
            "product": "shirt"


        },
        {
            "id": "7",
            "price": "$7.99",
            "product": "shirt"


        },
        {
            "id": "9",
            "price": "$9.99",
            "product": "shirt"


        },
        {
            "id": "9",
            "price": "$9.99",
            "product": "shirt"


        },
    ]

my_file = open("ex.txt", "r")
content = my_file.read()
content_list = content.split("\n")


# Generate the Json Web Token 
def generate_jwt(sa_keyfile,
                 sa_email,
                 audience,
                 expiry_length=3600):


    """Generates a signed JSON Web Token using a Google API Service Account."""


    now = int(time.time())


    # build payload
    payload = {
        'iat': now,
        # expires after 'expiry_length' seconds.
        "exp": now + expiry_length,
        # iss must match 'issuer' in the security configuration in your
        # swagger spec (e.g. service account email). It can be any string.
        'iss': sa_email,
        # aud must be either your Endpoints service name, or match the value
        # specified as the 'x-google-audience' in the OpenAPI document.
        'aud':  audience,
        # sub and email should match the service account's email address
        'sub': sa_email,
        'email': sa_email
    }


    # sign with keyfile
    signer = crypt.RSASigner.from_service_account_file(sa_keyfile)
    jwt_token = jwt.encode(signer, payload)
    #print(jwt_token.decode('utf-8'))
    return jwt_token




# Publish the message
def publish_with_jwt_request(signed_jwt, encoded_element, url):
    #Request Headers
    headers = {
        'Authorization': 'Bearer {}'.format(signed_jwt.decode('utf-8')),
        'content-type': 'application/json'
    }
    # Request json data
    json_data = {
        "messages": [
            {
                "data" : encoded_element,
                "ordering_key": "first order",
                "attributes" : {"somekey" : "somevalue"}
            }
        ]
    }


    # Get response data
    response = requests.post(url, headers=headers, json=json_data)
    print(response.status_code, response.content)
    response.raise_for_status()


# Get JWT 
signed_jwt = generate_jwt( sa_keyfile, sa_email, audience )

#encoding = topic.schema_settings.encoding
# Encode the data and publish it
for element in json_payload:
    #info = json.dumps(json.loads(element.replace("'",'"'))).encode("utf-8")
    dumped_element = json.dumps(element)
    message_bytes = dumped_element.encode('utf-8')
    base64_bytes = base64.b64encode(message_bytes)
    encoded_element = base64_bytes.decode("utf-8")
    print(encoded_element)
    publish_with_jwt_request(signed_jwt, encoded_element, url)
