# Azure ServiceBus clients review

## azure-servicebus

### PROS
1. Native Azure ServiceBus client.
2. Custom Azure ServiceBus features are implemented, i.e. session enable, etc. Amqp RFC can not to contain some of Azure ServiceBus features.
3. On deployment requires only requirements.txt dependencies.
4. Python 2.7, Python 3.6

### CONS
1. Uses only http. Does not support amqp. It slows performance.
2. Sends and receives only in blocked mode (synchronously).
3. Performance: sends 4.3 msg/sec, receives 6.8 msg/sec.


## python-qpid-proton

### PROS
1. It can be used in the widest range of messaging applications, including brokers, client libraries, routers, bridges, proxies, and more.
2. Uses both amqp and http protocols.
3. Sends and receives both in synchronous and asynchronous  mode.
4. Performance: in asynchronous mode sends 680 msg/sec, receives 55 msg/sec., in synchronous mode sends 6.6 msg/sec, receives 6.8 msg/sec.
5. Python 2.7, Python 3.6

### CONS
1. Does not support custom Azure ServiceBus features like creating topics, queues, it is difficult or impossible to pass SessionId (to send in queue where session enabled) in case it will be needed.
2. More difficult deployment process (need install additional tools), but doable.


## azure-uamqp-python

### PROS
1. Supported with Microsoft.
2. Use amqp 1.0.
3. On deployment requires only requirements.txt dependencies.
4. Sends and receives both in synchronous and asynchronous  mode.
5. In synchronous mode batches can be used. It increases performance.
6. Performance: 
    * sends in sync mode (without batches) 5.5 msg/sec, receives 6.6 msg/sec, 
    * sends in sync mode (with batches) 238 msg/sec (50 msg/batch), receives 418 msg/sec (300 msg/batch), 
    * sends in async mode 6.0 msg/sec, receives 457 msg/sec.

### CONS
1. Only Python 3.
2. Does not support custom Azure ServiceBus features like creating topics, queues, it is difficult or impossible to pass SessionId (to send in queue where session enabled) in case it will be needed.
