from __future__ import print_function, unicode_literals
import threading
import time
try:
    import Queue
except:
    import queue as Queue

from proton import Message


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

    def __init__(self, conn, sender):
        self.conn = conn
        self.sender = sender
        self.rate = PerfRate()
        threading.Thread.__init__(self)

    def run(self):
        i = 0
        while self.running:
            i += 1
            msg = Message(body=b'Test Message1')
            self.sender.send(msg)
            self.rate.increment()

    def stop(self):
        self.running = False


class PerfConsumerSync(threading.Thread):
    running = True

    def __init__(self, conn, receiver):
        self.conn = conn
        self.receiver = receiver
        self.rate = PerfRate()
        threading.Thread.__init__(self)

    def run(self):
        while self.running:
            text_message = self.receiver.receive()
            if text_message is not None:
                self.rate.increment()

    def stop(self):
        self.receiver.accept()
        self.running = False
