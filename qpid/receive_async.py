import json
import os
import random

from proton.handlers import MessagingHandler
from proton.reactor import Container, Handler, Copy
try:
    from urllib.parse import quote_plus
except ImportError:
    from urllib import quote_plus

import perftest


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

TOPIC_NAME = 'test_topic_4'
SUBSCRIPTION_NAME = 'client1'
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']

CONN_STR = 'amqps://{}:{}@{}.servicebus.windows.net'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE)


class Recurring(Handler):
    def __init__(self, period):
        self.period = period

    def on_reactor_init(self, event):
        self.container = event.reactor
        self.container.schedule(self.period, self)

    def on_timer_task(self, event):
        self.container.schedule(self.period, self)


class Recv(MessagingHandler):
    def __init__(self, url, path):
        super(Recv, self).__init__(auto_accept=False, auto_settle=False)
        self.url = url
        self.path = path
        self.rate = perftest.PerfRate()

    def on_start(self, event):
        self.container = event.container
        conn = event.container.connect(self.url, allowed_mechs='PLAIN')
        event.container.create_receiver(conn, self.path, options=Copy(),
                                        name=SUBSCRIPTION_NAME)
        self.container.schedule(1, self)

    def on_message(self, event):
        # msg = event.message
        # print(msg.body)
        self.rate.increment()
        if random.random() < 0.5:
            self.release(event.delivery, delivered=False)
        else:
            self.accept(event.delivery)

    def on_timer_task(self, event):
        print("consumer received {}".format(self.rate.print_rate()))
        self.container.schedule(1, self)

try:
    receiver = Recv(CONN_STR, TOPIC_NAME)
    consumer = Recurring(0.001)
    container = Container(receiver, consumer)
    container.run()
except KeyboardInterrupt:
    container.stop()
    print()
