import json
import os

import time
from queue import Queue
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container, Handler
from urllib.parse import quote_plus

import perftest


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
PAYLOAD = {
    'hello': 'world',
    'time': -1
}


class Producer(Handler):
    def __init__(self, period, queue):
        self.period = period
        self.queue = queue

    def on_reactor_init(self, event):
        self.container = event.reactor
        self.container.schedule(self.period, self)

    def on_timer_task(self, event):
        PAYLOAD['time'] = time.time()
        self.queue.put(PAYLOAD)
        self.container.schedule(self.period, self)


class Sender(MessagingHandler):
    def __init__(self, url, target):
        super(Sender, self).__init__()
        self.url = url
        self.target = target
        self.queue = Queue()
        self.rate = perftest.PerfRate()

    def on_start(self, event):
        self.container = event.container
        conn = self.container.connect(self.url, allowed_mechs='PLAIN')
        self.sender = self.container.create_sender(conn, self.target)

        self.container.schedule(1, self)

    def on_sendable(self, event):
        self.send()

    def send(self):
        while self.sender.credit and not self.queue.empty():
            msg = Message(body=self.queue.get(False))
            self.sender.send(msg)
            self.rate.increment()

    def on_accepted(self, event):
        pass

    def on_timer_task(self, event):
        print("producer sent {}".format(self.rate.print_rate()))
        self.send()
        self.container.schedule(1, self)


try:
    sender = Sender(CONN_STR, TOPIC_NAME)
    producer = Producer(0.001, sender.queue)
    Container(sender, producer).run()
except KeyboardInterrupt:
    pass
