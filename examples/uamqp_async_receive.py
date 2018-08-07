import asyncio
import json
import os

import uamqp
from uamqp import authentication
from urllib.parse import quote_plus


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)


TOPIC_NAME = config['topic_name']
SUBSCRIPTION_NAME = config['subscription_name']
SERVICE_NAMESPACE = config['service_namespace']
KEY_NAME = config['key_name']
KEY_VALUE = config['key_value']
HOSTNAME = '{}.servicebus.windows.net'.format(SERVICE_NAMESPACE)

uri = "sb://{}/{}".format(HOSTNAME, TOPIC_NAME)
target = "amqps://{}/{}".format(HOSTNAME, TOPIC_NAME)

CONN_STR = ('amqps://{}:{}@{}.servicebus.windows.net/{}/Subscriptions/{}'
            .format(KEY_NAME, quote_plus(KEY_VALUE, safe=''),
                    SERVICE_NAMESPACE, TOPIC_NAME, SUBSCRIPTION_NAME))


def on_message_received(message):
    annotations = message.annotations
    print("Sequence Number: {}".format(
        annotations.get(b'x-opt-sequence-number')))
    return message


async def receive_async(uri, key_name, key_value, conn_str):
    sas_auth = authentication.SASTokenAsync.from_shared_access_key(
        uri, key_name, key_value)

    receive_client = uamqp.ReceiveClientAsync(
        conn_str, auth=sas_auth, timeout=10, prefetch=10)
    print("Created client, receiving...")
    await receive_client.receive_messages_async(on_message_received)
    print("Finished receiving")


try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(receive_async(uri, KEY_NAME, KEY_VALUE, CONN_STR))

except KeyboardInterrupt:
    pass
