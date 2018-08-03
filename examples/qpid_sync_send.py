import json
import os

from proton import Message
from proton.utils import BlockingConnection


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

msg = Message(body="Qpid test message")
sender.send(msg)
print("Message sent")
