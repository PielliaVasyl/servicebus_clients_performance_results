from __future__ import print_function

import json
import optparse
import os

from proton.handlers import MessagingHandler
from proton.reactor import Container


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
address = conn_str + '/' + topic_name


class Recv(MessagingHandler):
    def __init__(self, url, count):
        super(Recv, self).__init__()
        self.url = url
        self.expected = count
        self.received = 0

    def on_start(self, event):
        print("on_start")
        event.container.create_receiver(self.url, name=subscription_name)
        print("on_start_end")

    def on_message(self, event):
        print("on_message")
        if event.message.id and event.message.id < self.received:
            # ignore duplicate message
            return
        if self.expected == 0 or self.received < self.expected:
            print(event.message.body)
            self.received += 1
            if self.received == self.expected:
                event.receiver.close()
                event.connection.close()


parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-a", "--address", default=address,
                  help="address from which messages are received "
                       "(default %default)")
parser.add_option("-m", "--messages", type="int", default=0,
                  help="number of messages to receive; 0 receives "
                       "indefinitely (default %default)")
opts, args = parser.parse_args()

try:
    Container(Recv(opts.address, opts.messages)).run()
except KeyboardInterrupt:
    pass
