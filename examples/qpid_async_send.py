from __future__ import print_function, unicode_literals

import json
import os

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from urllib.parse import quote_plus


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
ADDRESS = CONN_STR + '/' + TOPIC_NAME


class Send(MessagingHandler):
    def __init__(self, url, target, messages):
        super(Send, self).__init__()
        self.url = url
        self.sent = 0
        self.confirmed = 0
        self.total = messages
        self.target = target

    def on_start(self, event):
        self.container = event.container
        conn = self.container.connect(self.url, allowed_mechs='PLAIN')
        self.sender = self.container.create_sender(conn, self.target)

    def on_sendable(self, event):
        while event.sender.credit and self.sent < self.total:
            msg = Message(id=(self.sent + 1),
                          body={'sequence': (self.sent + 1)})
            event.sender.send(msg)
            self.sent += 1

    def on_accepted(self, event):
        self.confirmed += 1
        if self.confirmed == self.total:
            print("all messages confirmed")
            event.connection.close()

    def on_disconnected(self, event):
        self.sent = self.confirmed

try:
    Container(Send(ADDRESS, TOPIC_NAME, 100)).run()
except KeyboardInterrupt:
    pass
