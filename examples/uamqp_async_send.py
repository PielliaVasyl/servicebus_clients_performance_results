import asyncio
import json
import os

import uamqp
from uamqp import authentication


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)


TOPIC_NAME = config['topic_name']
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']
HOSTNAME = '{}.servicebus.windows.net'.format(SERVICE_NAMESPACE)

uri = "sb://{}/{}".format(HOSTNAME, TOPIC_NAME)
target = "amqps://{}/{}".format(HOSTNAME, TOPIC_NAME)


async def send_async(uri, key_name, key_value, target):
    sas_auth = authentication.SASTokenAsync.from_shared_access_key(
        uri, key_name, key_value)

    send_client = uamqp.SendClientAsync(target, auth=sas_auth, debug=False)

    while True:
        msg_content = b"hello world"
        message = uamqp.Message(msg_content)
        await send_client.send_message_async(message)
    await send_client.close_async()


try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_async(uri, KEY_NAME, KEY_VALUE, target))

except KeyboardInterrupt:
    pass
