from __future__ import print_function

import json
import os

from proton.handlers import MessagingHandler
from proton.reactor import Container, Copy
from urllib.parse import quote_plus


with open(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '../config.json')),
        'r') as read_file:
    config = json.load(read_file)

TOPIC_NAME = 'test_topic_4'
SUBSCRIPTION_NAME = 'client1'
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']

CONN_STR = 'amqps://{}:{}@{}.servicebus.windows.net'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE)
ADDRESS = CONN_STR + '/' + TOPIC_NAME


class Recv(MessagingHandler):
    def __init__(self, url, target):
        super(Recv, self).__init__(auto_accept=False)
        self.url = url
        self.target = target

    def on_start(self, event):
        conn = event.container.connect(self.url, allowed_mechs='PLAIN')
        event.container.create_receiver(conn, self.target, options=Copy(),
                                        name=SUBSCRIPTION_NAME)

    def on_message(self, event):
        msg = event.message
        print(msg.id, ',', msg.delivery_count)
        self.accept(event.delivery)
        # self.release(event.delivery, delivered=False)
        # or self.accept(event.delivery)

try:
    Container(Recv(ADDRESS, TOPIC_NAME)).run()
except KeyboardInterrupt:
    pass
