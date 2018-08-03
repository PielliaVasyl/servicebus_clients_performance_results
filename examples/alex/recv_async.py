import random
from urllib.parse import quote_plus
from proton.handlers import MessagingHandler
from proton.reactor import Container, Copy

KEY = '<SAS primary key>'
SERVER = ('amqps://<SAS policy name>:' +
          quote_plus(KEY, safe='') +
          '@<resource_name>.servicebus.windows.net')
QUEUE = '<queue_name>'


class Recv(MessagingHandler):
    def __init__(self, url, queue):
        super(Recv, self).__init__(auto_accept=False, auto_settle=False)
        self.url = url
        self.queue = queue

    def on_start(self, event):
        conn = event.container.connect(self.url, allowed_mechs='PLAIN')
        event.container.create_receiver(conn, self.queue, options=Copy())

    def on_message(self, event):
        msg = event.message
        print(msg.body)
        if random.random() < 0.9:
            self.release(event.delivery, delivered=False)
            print('release')
        else:
            self.accept(event.delivery)
            print('accept')


try:
    Container(Recv(SERVER, QUEUE)).run()
except KeyboardInterrupt:
    pass
