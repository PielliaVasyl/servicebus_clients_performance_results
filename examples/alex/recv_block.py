from urllib.parse import quote_plus
from proton.utils import BlockingConnection


KEY = '<SAS primary key>'
SERVER = ('amqps://<SAS policy name>:' +
          quote_plus(KEY, safe='') +
          '@<resource_name>.servicebus.windows.net')
QUEUE = '<queue_name>'

if __name__ == '__main__':
    conn = BlockingConnection(SERVER, allowed_mechs='PLAIN')

    receiver = conn.create_receiver(QUEUE)
    msg = receiver.receive(timeout=30)
    print(msg)
    receiver.accept()

    conn.close()
