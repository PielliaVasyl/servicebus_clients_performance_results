# producer sent 200 messages in 30.583856105804443 secs ;
# rate=6.5393977563882935 msgs/sec

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
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']

CONN_STR = 'amqps://{}:{}@{}.servicebus.windows.net'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE)

conn = BlockingConnection(CONN_STR, allowed_mechs='PLAIN')
sender = conn.create_sender(TOPIC_NAME)

producer = perftest.PerfProducer(conn, sender)
producer.start()

for i in range(1, 30):
    time.sleep(1)
    print ("producer sent {}".format(producer.rate.print_rate()))

producer.stop()
