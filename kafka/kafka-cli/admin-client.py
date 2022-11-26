#!/usr/bin/env python

import argparse
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType

parser = argparse.ArgumentParser(description="Consumer")
parser.add_argument('--broker', '-b', default='localhost:9092', help='broker host info: [IP]:[PORT]')

parser.add_argument('--topics', help="Topic list", action="store_true")
parser.add_argument('--groups', help="Group List", action="store_true")

parser.add_argument('--describe-topic', help="Topic Describe", action="store_true")
parser.add_argument('--describe-group', help="Group Describe", action="store_true")

parser.add_argument('--topic', '-t', default='test', help='Topic Name')
parser.add_argument('--group', '-g', default=None, help="Group Name")

args = parser.parse_args()

admin_client = KafkaAdminClient(
    bootstrap_servers=args.broker,
)

if args.topics:
    topics = admin_client.list_topics()
    for topic in topics:
        print(topic)
elif args.groups:
    groups = admin_client.list_consumer_groups(args.group)
    for group in groups:
        print(groups)
elif args.describe_topic:
    topics = []
    topics.append(args.topic)
    result = admin_client.describe_topics(topics)
    print(result)
elif args.describe_group:
    result = admin_client.list_consumer_group_offsets(group_id=args.group)
    for item in result:
        print(item)


admin_client.close()