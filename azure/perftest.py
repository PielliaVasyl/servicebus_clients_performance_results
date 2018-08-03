import json
import os
import time
import threading

from azure.servicebus import Message

with open(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       '../config.json')
                          ), 'r') as read_file:
    config = json.load(read_file)

topic_name = config['topic_name']
subscription_name = config['subscription_name']


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

    def __init__(self, bus_service):
        self.bus_service = bus_service
        self.rate = PerfRate()
        threading.Thread.__init__(self)

    def run(self):
        i = 0
        while self.running:
            i += 1
            msg = Message(b'Test Message1')
            self.bus_service.send_topic_message(topic_name, msg)
            self.rate.increment()

    def stop(self):
        self.running = False


class PerfConsumerSync(threading.Thread):
    running = True

    def __init__(self, bus_service):
        self.bus_service = bus_service
        self.rate = PerfRate()
        threading.Thread.__init__(self)

    def run(self):
        while self.running:
            text_message = self.bus_service.receive_subscription_message(
                topic_name, subscription_name)
            if text_message is not None:
                self.rate.increment()

    def stop(self):
        self.running = False
