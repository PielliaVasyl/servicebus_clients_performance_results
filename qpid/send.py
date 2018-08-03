# producer sent 200 messages in 30.583856105804443 secs ;
# rate=6.5393977563882935 msgs/sec

import json
import os
import time

from proton.utils import BlockingConnection

import perftest


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

topic_name = config['topic_name']
service_namespace = config['service_namespace']
key_name = config['key_name']
key_value = config['key_value']
conn_str = f'amqps://{key_name}:{key_value}@{service_namespace}.servicebus.windows.net'

conn = BlockingConnection(conn_str, allowed_mechs='PLAIN')
sender = conn.create_sender(topic_name)

producer = perftest.PerfProducer(conn, sender)
producer.start()

for i in range(1, 30):
    time.sleep(1)
    print ("producer sent {}".format(producer.rate.print_rate()))

producer.stop()
