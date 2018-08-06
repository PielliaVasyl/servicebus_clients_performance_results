# consumer received 217 messages in 30.603073120117188 secs ;
# rate=7.090791148597205 msgs/sec

import json
import os
import time

from proton.utils import BlockingConnection
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

CONN_STR = 'amqps://{}:{}@{}.servicebus.windows.net'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE)

conn = BlockingConnection(CONN_STR, allowed_mechs='PLAIN')
receiver = conn.create_receiver(TOPIC_NAME, name=SUBSCRIPTION_NAME)

consumer = perftest.PerfConsumerSync(conn, receiver)
consumer.start()

for i in range(1, 30):
    time.sleep(1)
    print("consumer received " + consumer.rate.print_rate())

consumer.stop()
