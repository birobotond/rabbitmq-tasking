#!/usr/bin/env python
import pika
import sys


def run(channel, exchange='autoteile'):
    import time
    import random
    import string

    channel.exchange_declare(exchange=exchange, exchange_type='direct', durable=True)

    tubes = {
        'product': {'channel': 'products', 'seed': 3000000},
        'offer': {'channel': 'offers', 'seed': 'choices'},
        'image': {'channel': 'images', 'seed': 12},
    }

    print('Declare tubes')
    for key, tube in tubes.items():
        ch = tube['channel'] if 'channel' in tube else ''

        if not ch:
            print('...skipping')
            continue

        channel.queue_declare(queue=ch)

    counter = 0

    try:
        while True:

            messages = []

            for key, tube in tubes.items():

                seed = tube['seed'] if 'seed' in tube else ''
                ch = tube['channel'] if 'channel' in tube else ''

                if not ch or not seed:
                    continue

                if seed == 'choices':
                    r = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(12))
                else:
                    r = random.randint(0, seed)

                message = tube
                message['body'] = '%s: %s' % (ch, r)
                messages.append(message)

            for message in messages:

                routing_key = message['channel'] if 'channel' in message else ''
                message_body = message['body'] if 'body' in message else ''
                if not routing_key or not message_body:
                    print('...skipping')
                    continue

                sent = channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=message_body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    )
                )

                if sent:
                    print('.', end='', flush=True)
                else:
                    print(message)

                # time.sleep(1)

            counter += len(messages)

    except KeyboardInterrupt:
        print('Exiting...')
        return counter


print('Producer started\n')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
run(connection.channel())
connection.close()

exit('Done')
