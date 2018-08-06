Azure ServiceBus clients review

azure-servicebus

PROS
Native Azure ServiceBus client.
Custom Azure ServiceBus features are implemented, i.e. session enable, etc. Amqp RFC can not to contain some of Azure ServiceBus features.
On deployment requires only requirements.txt dependencies.
Python 2.7, Python 3.6

CONS
Uses only http. Does not support amqp. It slows performance.
Sends and receives only in blocked mode (synchronously).
Performance: sends 4.3 msg/sec, receives 6.8 msg/sec.


python-qpid-proton

PROS
It can be used in the widest range of messaging applications, including brokers, client libraries, routers, bridges, proxies, and more.
Uses both amqp and http protocols.
Sends and receives both in synchronous and asynchronous  mode.
Performance: in asynchronous mode sends 680 msg/sec, receives 55 msg/sec., in synchronous mode sends 6.6 msg/sec, receives 6.8 msg/sec.
Python 2.7, Python 3.6

CONS
Does not support custom Azure ServiceBus features like creating topics, queues, it is difficult or impossible to pass SessionId (to send in queue where session enabled) in case it will be needed.
More difficult deployment process (need install additional tools), but doable.
