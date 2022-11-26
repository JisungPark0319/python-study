import argparse
from Manager.KafkaManager import KafkaManager


def main():
    parser = argparse.ArgumentParser(description="Kafka Manager v1.0")

    parser.add_argument('--broker', '-b', default='192.168.1.200:30550', help='broker host info: [IP]:[PORT]')
    parser.add_argument('--list', '-l', action='store_true', help='topic list print')
    parser.add_argument('--create', '-c', action='store_true', help='topic create')
    parser.add_argument('--partition', '-p', default=8, help='topic create partition num')
    parser.add_argument('--delete', '-d', action='store_true', help='topic delete')
    parser.add_argument('--send', '-s', action='store_true', help='send')
    parser.add_argument('--read', '-r', action='store_true', help='read')
    parser.add_argument('--topic', '-t', help='topic name')
    parser.add_argument('--data', '-v', help='data')
    parser.add_argument('--path', '-P', help='json file path')

    args = parser.parse_args()

    manager = KafkaManager(args)
    if args.list:
        manager.topic_list()
    if args.create:
        manager.topic_create()
        manager.topic_list()
    if args.delete:
        manager.topic_delete()
        manager.topic_list()
    if args.send:
        if args.path:
            manager.json_send()
        else:
            manager.send()
    if args.read:
        manager.read()


if __name__ == '__main__':
    main()
