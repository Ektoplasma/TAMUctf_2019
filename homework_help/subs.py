#!/usr/bin/env python
import pika
import os
import json
MQ_SERVER = os.environ.get("MQ_SERVER", "172.30.0.4")
MQ_PORT = int(os.environ.get("MQ_PORT", 5671))
MQ_SSL = os.environ.get("MQ_SSL", "true").lower() == "true"
CA_CERT = os.environ.get("CA_CERT", None)

if MQ_SSL:
    ssl_opts = {'ca_certs': CA_CERT}
else:
    ssl_opts = None

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=MQ_SERVER,
        port=MQ_PORT,
        ssl=MQ_SSL,
        ssl_options=ssl_opts,
        heartbeat=0,
        blocked_connection_timeout=60,
        connection_attempts=10,
        retry_delay=3
    ))
channel = connection.channel()

channel.queue_declare(queue='submissions')

def unmarshal(data):
    map = json.loads(data)
    if not isinstance(map, dict):
        raise ValueError(f'decoded data is not a dict: {type(map)}')

    result = [map.pop('user'),
        map.pop('assignment'),
        map.pop('code')]
    
    if map:
        raise ValueError(f"data contains extra keys: {list(map.keys())}")
    return result

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)

def process(method,props,body):
    print(method)
    print(props)
    print(body)
    print("Validate that we have information to return this request.")
    reqid, resp_queue = props.correlation_id, props.reply_to
    if not reqid  or not resp_queue:
        print("discarding message with invalid metadata:")
        return
    print("Validate that the correlation id is unique.")
    print("Unmarhsal the request")
    try:
        req = unmarshal(body)
        print(req)
    except ValueError as err:
        print("discarding invalid request ")
    return

for method, props, body in channel.consume(queue='submissions'):
    if any(val is None for val in (method, props, body)):
        break
    if process(method, props, body):
        channel.basic_ack(delivery_tag=method.delivery_tag)
    else:
        channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

