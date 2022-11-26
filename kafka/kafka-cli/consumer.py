#!/usr/bin/env python

import argparse
from kafka import KafkaConsumer

parser = argparse.ArgumentParser(description="Consumer")
parser.add_argument('--broker', '-b', default='localhost:9092', help='broker host info: [IP]:[PORT]')
parser.add_argument('--flow', '-f', action='store_true')
parser.add_argument('--topic', '-t', help='Topic Name', required=True)
parser.add_argument('--groupid', '-g', default=None, help="Group ID")

args = parser.parse_args()

if args.flow:
    consumer_timeout_ms = float('inf')
else:
    consumer_timeout_ms = 1000

consumer = KafkaConsumer(args.topic, 
            bootstrap_servers=args.broker,
            group_id=args.groupid,
            auto_offset_reset="earliest",
            consumer_timeout_ms=consumer_timeout_ms)

for message in consumer:
    print("partition={0} | offset={1} | headers={2} | value={3}".format(
        message.partition, message.offset, message.headers, message.value
    ))

consumer.close()