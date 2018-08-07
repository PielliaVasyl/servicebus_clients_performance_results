import json
import os
import time

import uamqp
from uamqp import authentication
from urllib.parse import quote_plus

import perftest

with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

TOPIC_NAME = config['topic_name']
SUBSCRIPTION_NAME = config['subscription_name']
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']

CONN_STR = 'amqps://{}:{}@{}.servicebus.windows.net/{}/Subscriptions/{}'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE, TOPIC_NAME,
    SUBSCRIPTION_NAME)
hostname = '{}.servicebus.windows.net'.format(SERVICE_NAMESPACE)

sas_auth = authentication.SASTokenAuth.from_shared_access_key(
        CONN_STR, KEY_NAME, KEY_VALUE)

receive_client = uamqp.ReceiveClient(
    CONN_STR, auth=sas_auth, debug=False, timeout=0, prefetch=1)

consumer = perftest.PerfConsumerSync(receive_client)
consumer.start()

for i in range(1, 30):
    time.sleep(1)
    print("consumer received " + consumer.rate.print_rate())

consumer.stop()
