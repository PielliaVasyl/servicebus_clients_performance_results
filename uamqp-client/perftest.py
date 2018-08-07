import json
import os
import time
import threading

import uamqp

with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

TOPIC_NAME = config['topic_name']
SUBSCRIPTION_NAME = config['subscription_name']


class PerfRate:
    count = 0
    start_time = time.time()
    
    def print_rate(self):
        end_time = time.time()
        period = end_time - self.start_time
        rate = self.count / (end_time - self.start_time)
        return "".join([str(self.count), " messages in ", str(period),
                        " secs ; rate=" + str(rate) + " msgs/sec"])
    
    def increment(self):
        self.count += 1


class PerfProducer(threading.Thread):
    running = True

    def __init__(self, send_client, use_batch=False):
        self.send_client = send_client
        self.use_batch = use_batch
        self.rate = PerfRate()
        threading.Thread.__init__(self)

    def run(self):
        while self.running:
            if not self.use_batch:
                msg_content = b"Hello world"
                msg = uamqp.Message(msg_content)
                self.send_client.queue_message(msg)
                results = self.send_client.send_all_messages(
                    close_on_done=False)
                assert not [m for m in results if (
                    m == uamqp.constants.MessageState.SendFailed)]
                self.rate.increment()
            else:
                message_batch = uamqp.message.BatchMessage(
                    self.data_generator())
                self.send_client.queue_message(message_batch)
                results = self.send_client.send_all_messages(
                    close_on_done=False)
                assert not [m for m in results if (
                    m == uamqp.constants.MessageState.SendFailed)]

    def stop(self):
        self.running = False
        self.send_client.close()

    def data_generator(self):
        for i in range(50):
            msg_content = "Hello world {}".format(i).encode('utf-8')
            self.rate.increment()
            yield msg_content


class PerfConsumerSync(threading.Thread):
    running = True

    def __init__(self, receive_client):
        self.receive_client = receive_client
        self.rate = PerfRate()
        threading.Thread.__init__(self)

    def run(self):
        while self.running:
            batch = self.receive_client.receive_message_batch()
            while batch:
                for message in batch:
                    self.rate.increment()
                batch = self.receive_client.receive_message_batch()

    def stop(self):
        self.running = False
        self.receive_client.close()
