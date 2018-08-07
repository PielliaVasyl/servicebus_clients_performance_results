# consumer received 201 messages in 30.54362916946411 secs ;
# rate=6.580750404112065 msgs/sec

import json
import os
import time

from azure.servicebus import ServiceBusService

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

bus_service = ServiceBusService(
    service_namespace=service_namespace,
    shared_access_key_name=key_name,
    shared_access_key_value=key_value)

bus_service.create_subscription(topic_name, subscription_name)

consumer = perftest.PerfConsumerSync(bus_service)
consumer.start()

for i in range(1, 30):
    time.sleep(1)
    print("consumer received " + consumer.rate.print_rate())

consumer.stop()
