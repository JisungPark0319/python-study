#! /usr/bin/env python
import argparse
import json
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description="Producer")

parser.add_argument('--broker', '-b', default='localhost:9092', help='broker host info: [IP]:[PORT]')
parser.add_argument('--topic', '-t', default='test', help='Topic Name', required=True)
parser.add_argument('--data', '-d', help='Message Data')
parser.add_argument('--file', '-f', help='Send data from json file')

args = parser.parse_args()

message_topic = args.topic
if not args.data and not args.file:
    print('Data or file flags are required')
    exit()

producer = KafkaProducer(
        bootstrap_servers=args.broker
        )

if args.data:    
    producer.send(message_topic, bytes(args.data, 'utf-8'))

if args.file:
    file_path = args.file
    try:
        with open(file_path, "r") as f:
            json_temp = json.load(f)
            json_data = json.dumps(json_temp)
            producer.send(message_topic, bytes(json_data, 'utf-8'))
    except ValueError:
        print("Json file format error")

producer.flush()
producer.close()

