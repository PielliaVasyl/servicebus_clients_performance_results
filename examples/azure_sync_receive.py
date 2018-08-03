import json
import os

from azure.servicebus import ServiceBusService

with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

topic_name = config['topic_name']
subscription_name = config['subscription_name']
service_namespace = config['service_namespace']
key_name = config['key_name']
key_value = config['key_value']

sbs = ServiceBusService(
    service_namespace=service_namespace,
    shared_access_key_name=key_name,
    shared_access_key_value=key_value)

sbs.create_subscription(topic_name, subscription_name)

try:
    while True:
        # peek_lock:
        #     Optional. True to retrieve and lock the message. False to read
        #     and delete the message. Default is True (lock).
        msg = sbs.receive_subscription_message(topic_name, subscription_name)
        if msg is not None:
            print("Azure package: message received: ", msg.body)
except KeyboardInterrupt:
    pass
