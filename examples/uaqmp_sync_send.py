import json
import os

import uamqp
from uamqp import authentication
from urllib.parse import quote_plus


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

TOPIC_NAME = config['topic_name']
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']
HOSTNAME = '{}.servicebus.windows.net'.format(SERVICE_NAMESPACE)

CONN_STR = 'amqps://{}:{}@{}.servicebus.windows.net/{}'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE, TOPIC_NAME)
target = "amqps://{}/{}".format(HOSTNAME, TOPIC_NAME)

sas_auth = authentication.SASTokenAuth.from_shared_access_key(
        CONN_STR, KEY_NAME, KEY_VALUE)
send_client = uamqp.SendClient(target, auth=sas_auth, debug=False)

msg_content = b"Hello world"
msg = uamqp.Message(msg_content)

send_client.queue_message(msg)
send_client.send_all_messages()
