import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json
from dotenv import load_dotenv
import os
import jwt
import requests
import time
import json
from jwt import algorithms


# load environment variables
load_dotenv()
CLIENT_ID = os.environ["CLIENT_ID"]
USER = os.environ["USERNAME"]
PRIVATE_KEY_FILE = '/Users/arupsarkar/Projects/Keys/AzureCloudFunction/server.key'
print(f"CLIENT_ID: {CLIENT_ID}")
print(f"USER: {USER}")
LOGIN_URL = os.environ["LOGIN_URL"]
TOKEN_URL = f'{LOGIN_URL}/services/oauth2/token'

# Create a semaphore to control access to the latest_replay_id variable
# makes it thread safe
semaphore = threading.Semaphore(1)
latest_replay_id = None


# Create a JWT token
def create_jwt():
    current_time = int(time.time())
    header = {
        "alg": "RS256"
    }
    payload = {
        "iss": CLIENT_ID,
        "sub": USER,
        "aud": LOGIN_URL,
        "exp": current_time + 3600
    }
    with open(PRIVATE_KEY_FILE, 'r') as key_file:
        private_key = key_file.read()

    token = jwt.encode(payload, private_key, algorithm='RS256', headers=header)
    return token
# get the org id
def get_org_id(access_token, instance_url):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    query_url = f'{instance_url}/services/data/v51.0/query/?q=SELECT+Id+FROM+Organization'
    
    response = requests.get(query_url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        org_id = response_data['records'][0]['Id']
        return org_id
    else:
        raise Exception(f"Failed to query organization ID: {response.content}")
# Get the access token
def get_access_token(jwt_token):
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
        'assertion': jwt_token
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(TOKEN_URL, data=data, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        # print with new line and indent
        print("\n".join(json.dumps(response_data, indent=2).split("\n")))
        return response_data['access_token'], response_data['instance_url']
    else:
        raise Exception(f"Failed to obtain access token: {response.content}")

# Check if the token is about to expire and refresh it if needed
def check_and_refresh_token(jwt_token, access_token, instance_url):
    # Print the current time and check if the token is about to expire
    print(f"Current time: {int(time.time())}")
    decoded_token = jwt.decode(jwt_token, options={"verify_signature": False})
    exp_time = decoded_token['exp']
    current_time = int(time.time())
    if exp_time - current_time < 120:  # Refresh 2 minutes before expiry
        jwt_token = create_jwt()
        access_token, instance_url = get_access_token(jwt_token)
        print("Access token refreshed")
    return jwt_token, access_token, instance_url
# def main():
#     try:
#         jwt_token = create_jwt()
#         access_token, instance_url = get_access_token(jwt_token)
#         print(f"Access Token: {access_token}")
#         print(f"Instance URL: {instance_url}")        
#         # Example: Making a request to Salesforce
#         headers = {
#             'Authorization': f'Bearer {access_token}',
#             'Content-Type': 'application/json'
#         }
#         response = requests.get(f'{instance_url}/services/data/v51.0/', headers=headers)
#         print(json.dumps(response.json(), indent=2))
#     except Exception as e:
#         print(f"Error: {e}")

# if __name__ == "__main__":
#     main()

# fetch request stream
def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)
# decode the payload
def decode(schema, payload):
  schema = avro.schema.parse(schema)
  buf = io.BytesIO(payload)
  decoder = avro.io.BinaryDecoder(buf)
  reader = avro.io.DatumReader(schema)
  ret = reader.read(decoder)
  return ret

# def connect_to_pubsub(instance_url, access_token):
#     # Create channel with proper credentials
#     composite_channel_credentials = grpc.ssl_channel_credentials()
#     auth_credentials = grpc.access_token_call_credentials(access_token)
#     channel_credentials = grpc.composite_channel_credentials(composite_channel_credentials, auth_credentials)
    
#     channel = grpc.secure_channel(f'{instance_url}:443', channel_credentials)
#     stub = pb2_grpc.PubSubStub(channel)
    
#     return stub

# def subscribe_to_event(stub, access_token):
#     headers = [
#         ('Authorization', f'Bearer {access_token}'),
#         ('instanceurl', instance_url)
#     ]
    
#     request = pb2.StreamingRequest(
#         topic_name='/data/AccountChangeEvent'
#     )
    
#     metadata = (
#         ('Authorization', f'Bearer {access_token}'),
#         ('instanceurl', instance_url),
#     )

#     try:
#         for response in stub.Subscribe(request, metadata=metadata):
#             print(response)
#     except grpc.RpcError as e:
#         print(f"Error: {e}")

# gRPC docs for Python: https://grpc.io/docs/languages/python/basics/
# Salesforce docs: https://developer.salesforce.com/docs/platform/pub-sub-api/guide/qs-generate-stub.html        
with open(certifi.where(), 'rb') as f:
    creds = grpc.ssl_channel_credentials(f.read())
with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
    # All of the code in the rest of the tutorial will go inside this block.
    # Make sure that the indentation of the new code you add starts from this comment’s block.
    try:
        jwt_token = create_jwt()
        access_token, instance_url = get_access_token(jwt_token)
        print(f"Access Token: {access_token}")
        print(f"Instance URL: {instance_url}")        
        # Example: Making a request to Salesforce
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        # get the org id
        org_id = get_org_id(access_token, instance_url)
        # response = requests.get(f'{instance_url}/services/data/v51.0/', headers=headers)
        # print(json.dumps(response.json(), indent=2))
        print(f"Org ID: {org_id}")
        sessionid = access_token
        instanceurl = instance_url
        tenantid = org_id
        authmetadata = (('accesstoken', sessionid),
            ('instanceurl', instanceurl),
            ('tenantid', tenantid))
        # orint authmetadata
        print(authmetadata)
        stub = pb2_grpc.PubSubStub(channel)
        print("Connected to PubSub")
        # print stub details
        print(stub)

        # stub = connect_to_pubsub(instance_url, access_token)
        print("Connected to PubSub")
        
        metadata = (
            ('Authorization', f'Bearer {access_token}'),
            ('instanceurl', instance_url),
        )        
        # subscribe_to_event(stub, access_token)
        #mysubtopic = "/data/AccountChangeEvent"
        mysubtopic = "/event/azure_iot__e"
        print('Subscribing to ' + mysubtopic)
        substream = stub.Subscribe(fetchReqStream(mysubtopic),
                metadata=authmetadata)
        for event in substream:

            jwt_token, access_token, instance_url = check_and_refresh_token(jwt_token, access_token, instance_url)
            authmetadata = (
                    ('accesstoken', access_token),
                    ('instanceurl', instance_url),
                    ('tenantid', org_id)
                )            
            if event.events:
                semaphore.release()
                print("Number of events received: ", len(event.events))
                payloadbytes = event.events[0].event.payload
                schemaid = event.events[0].event.schema_id
                schema = stub.GetSchema(
                        pb2.SchemaRequest(schema_id=schemaid),
                        metadata=authmetadata
                    ).schema_json
                decoded = decode(schema, payloadbytes)
                print("Got an event!", json.dumps(decoded))
            else:
                print("[", time.strftime('%b %d, %Y %l:%M%p %Z'),
                    "] The subscription is active.")
            latest_replay_id = event.latest_replay_id

    except Exception as e:
        print(f"Error: {e}")        