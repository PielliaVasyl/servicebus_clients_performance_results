import random
import json
import os
from urllib.parse import quote_plus
from proton.handlers import MessagingHandler
from proton.reactor import Container, Copy

# KEY = '<SAS primary key>'
# SERVER = ('amqps://<SAS policy name>:' +
#           quote_plus(KEY, safe='') +
#           '@<resource_name>.servicebus.windows.net')
QUEUE = 'test_topic_4'
with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']

CONNECTION_URL = 'amqps://{}:{}@{}.servicebus.windows.net'.format(
    KEY_NAME, quote_plus(KEY_VALUE, safe=''), SERVICE_NAMESPACE)


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
    Container(Recv(CONNECTION_URL, QUEUE)).run()
except KeyboardInterrupt:
    pass
