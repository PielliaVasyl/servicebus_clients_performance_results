import os
import json

import uamqp
from uamqp import authentication
from urllib.parse import quote_plus

with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

TOPIC_NAME = config['topic_name']
SUBSCRIPTION_NAME = config['subscription_name']
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']

CONN_STR = ('amqps://{}:{}@{}.servicebus.windows.net/{}/Subscriptions/{}'
            .format(KEY_NAME, quote_plus(KEY_VALUE, safe=''),
                    SERVICE_NAMESPACE, TOPIC_NAME,SUBSCRIPTION_NAME))
hostname = '{}.servicebus.windows.net'.format(SERVICE_NAMESPACE)

sas_auth = authentication.SASTokenAuth.from_shared_access_key(
        CONN_STR, KEY_NAME, KEY_VALUE)
# Or
# plain_auth = authentication.SASLPlain(hostname, KEY_NAME, KEY_VALUE)

receive_client = uamqp.ReceiveClient(
    CONN_STR, auth=sas_auth, debug=False, timeout=0, prefetch=1)
message_batch = receive_client.receive_message_batch(max_batch_size=1)
message = message_batch[0]
# Or
# message = uamqp.receive_message(CONN_STR, auth=plain_auth)

print("Received: {}".format(message))
