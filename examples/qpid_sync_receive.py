import json
import os

from proton.utils import BlockingConnection


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

try:
    while True:
        msg = receiver.receive()
        # Methods receiver.accept() or receiver.reject() are used after
        # receiver.receive()
        if msg is not None:
            print("Qpid package: Message received: ", msg.body)
except KeyboardInterrupt:
    pass
