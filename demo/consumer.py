#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='autoteile', exchange_type='direct', durable=True)

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

severities = ['products', 'offers']

for severity in severities:
    channel.queue_bind(exchange='autoteile',
                       queue=queue_name,
                       routing_key=severity)

print(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(
    callback,
    queue=queue_name
)

channel.start_consuming()
