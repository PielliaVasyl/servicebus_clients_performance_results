from __future__ import print_function, unicode_literals

import json
import os

from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)


topic_name = config['topic_name']
service_namespace = config['service_namespace']
key_name = config['key_name']
key_value = config['key_value']
conn_str = f'amqps://{key_name}:{key_value}@{service_namespace}.servicebus.windows.net'


class Send(MessagingHandler):
    def __init__(self, url, messages):
        super(Send, self).__init__()
        self.url = url
        self.sent = 0
        self.confirmed = 0
        self.total = messages

    def on_start(self, event):
        print('start')
        event.container.create_sender(self.url)

    def on_sendable(self, event):
        print('sendable')
        while event.sender.credit and self.sent < self.total:
            msg = Message(id=(self.sent+1), body={'sequence':(self.sent+1)})
            event.sender.send(msg)
            self.sent += 1

    def on_accepted(self, event):
        print('accepted')
        self.confirmed += 1
        if self.confirmed == self.total:
            print("all messages confirmed")
            event.connection.close()

    def on_disconnected(self, event):
        self.sent = self.confirmed

Container(Send(conn_str, 100), allowed_mechs='PLAIN').run()
