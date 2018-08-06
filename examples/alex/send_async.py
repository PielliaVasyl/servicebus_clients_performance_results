import time
from urllib.parse import quote_plus
from queue import Queue
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container, Handler

KEY = '<SAS primary key>'
SERVER = ('amqps://<SAS policy name>:' +
          quote_plus(KEY, safe='') +
          '@<resource_name>.servicebus.windows.net')
QUEUE = '<queue_name>'
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
        print("Tick...")
        PAYLOAD['time'] = time.time()
        self.queue.put(PAYLOAD)
        self.container.schedule(self.period, self)


class Sender(MessagingHandler):
    def __init__(self, url, target):
        super(Sender, self).__init__()
        self.url = url
        self.target = target
        self.queue = Queue()

    def on_start(self, event):
        self.container = event.container
        conn = self.container.connect(self.url, allowed_mechs='PLAIN')
        self.sender = self.container.create_sender(conn, self.target)

        self.container.schedule(5, self)

    def on_sendable(self, event):
        self.send()

    def send(self):
        while self.sender.credit and not self.queue.empty():
            print('Sending msg')
            msg = Message(body=self.queue.get(False))
            self.sender.send(msg)

    def on_accepted(self, event):
        print('Message accepted')

    def on_timer_task(self, event):
        print('Checking for new data...')
        self.send()
        self.container.schedule(5, self)


try:
    sender = Sender(SERVER, QUEUE)
    producer = Producer(2.0, sender.queue)
    Container(sender, producer).run()
except KeyboardInterrupt:
    pass
