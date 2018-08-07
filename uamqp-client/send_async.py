import asyncio
import json
import os
import time

import uamqp
from uamqp import authentication


with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)


async def send_async():
    TOPIC_NAME = config['topic_name']
    SERVICE_NAMESPACE = config['service_namespace']
    KEY_NAME = config['key_name']
    KEY_VALUE = config['key_value']
    HOSTNAME = '{}.servicebus.windows.net'.format(SERVICE_NAMESPACE)

    uri = "sb://{}/{}".format(HOSTNAME, TOPIC_NAME)
    sas_auth = authentication.SASTokenAsync.from_shared_access_key(
        uri, KEY_NAME, KEY_VALUE)

    target = "amqps://{}/{}".format(HOSTNAME, TOPIC_NAME)
    send_client = uamqp.SendClientAsync(target, auth=sas_auth, debug=False)
    start_time = time.time()
    count = 0
    time_printed = 0

    while True:
        count += 1
        end_time = time.time()
        period = end_time - start_time
        if time_printed < int(period):
            time_printed = int(period)
            rate = count / (end_time - start_time)
            print("".join([str(count), " messages in ", str(period),
                           " secs ; rate=" + str(rate) + " msgs/sec"]))
        msg_content = b"hello world"
        message = uamqp.Message(msg_content)
        await send_client.send_message_async(message)
    await send_client.close_async()



try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_async())

except KeyboardInterrupt:
    pass
