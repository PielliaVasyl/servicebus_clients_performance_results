from urllib.parse import quote_plus
from proton import Message
from proton.utils import BlockingConnection

KEY = '<SAS primary key>'
SERVER = ('amqps://<SAS policy name>:' +
          quote_plus(KEY, safe='') +
          '@<resource_name>.servicebus.windows.net')
QUEUE = '<queue_name>'
PAYLOAD = {
        'hello': 'world',
}


if __name__ == '__main__':
    conn = BlockingConnection(SERVER, allowed_mechs='PLAIN')
    sender = conn.create_sender(QUEUE)
    sender.send(Message(body=PAYLOAD))
    conn.close()
