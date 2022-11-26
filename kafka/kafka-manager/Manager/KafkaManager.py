from SimpleKafka.SimpleKafka import *
import json


class KafkaManager:
    def __init__(self, args):
        self.args = args

    def topic_list(self):
        client = SimpleClient(self.args.broker)
        client.list_topics()
        client.close()

    def describe_topics(self):
        client = SimpleClient(self.args.broker)
        client.describe_topics(self.args.topic)
        client.close()

    def topic_create(self):
        client = SimpleClient(self.args.broker)
        client.create_topic(self.args.topic, self.args.partition)
        client.close()

    def topic_delete(self):
        client = SimpleClient(self.args.broker)
        client.delete_topic(self.args.topic)
        client.close()

    def send(self):
        producer = SimpleProducer(self.args.broker)
        producer.send(self.args.topic, self.args.data)
        producer.close()

    def json_send(self):
	file_path = self.args.path
        producer = SimpleProducer(self.args.broker)
        try:
            with open(file_path, "r") as f:
                json_temp = json.load(f)
                json_data = json.dumps(json_temp)
                producer.send(self.args.topic, json_data)
        except ValueError:
            print("Json File Format Error")

        producer.close()

    def read(self):
        consumer = SimpleConsumer(self.args.topic, self.args.broker)
        consumer.read()
        consumer.close()


def main():
    pass


if __name__ == "__main__":
    main()
