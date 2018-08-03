# consumer received 217 messages in 30.603073120117188 secs ;
# rate=7.090791148597205 msgs/sec

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
subscription_name = config['subscription_name']
service_namespace = config['service_namespace']
key_name = config['key_name']
key_value = config['key_value']
conn_str = f'amqps://{key_name}:{key_value}@{service_namespace}.servicebus.windows.net'

conn = BlockingConnection(conn_str, allowed_mechs='PLAIN')
receiver = conn.create_receiver(topic_name, name=subscription_name)

consumer = perftest.PerfConsumerSync(conn, receiver)
consumer.start()

for i in range(1, 30):
    time.sleep(1)
    print("consumer received " + consumer.rate.print_rate())

consumer.stop()
