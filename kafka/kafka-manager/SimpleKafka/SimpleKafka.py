from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import *
from kafka import KafkaConsumer
from kafka import KafkaProducer


class SimpleClient:
    def __init__(self, brokers):
        self.bootstrap_servers = brokers
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=str(self.bootstrap_servers)
        )

    def list_topics(self):
        print(self.admin_client.list_topics())

    def describe_topics(self, topic):
        print(self.admin_client.describe_topics(topic))

    def create_topic(self, topic, partitions=8, replication=1):
        try:
            topic_list = [NewTopic(
                name=str(topic),
                num_partitions=int(partitions),
                replication_factor=int(replication)
            )]
            self.admin_client.create_topics(
                new_topics=topic_list,
                validate_only=False
            )
        except TopicAlreadyExistsError:
            print("ERROR: The topic exists")

    def delete_topic(self, topic):
        try:
            self.admin_client.delete_topics([str(topic)])
        except UnknownTopicOrPartitionError:
            print("ERROR: The topic does not exists")

    def close(self):
        self.admin_client.close()


class SimpleConsumer:
    def __init__(self, topic, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000
        )

    def read(self):
        for message in self.consumer:
            print("partition={0} | offset={1} | headers={2} | value={3}".format(
                message.partition, message.offset, message.headers, message.value
            ))

    def close(self):
        self.consumer.close()


class SimpleProducer:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(bootstrap_servers=[self.bootstrap_servers])

    def send(self, topic, data):
        self.producer.send(topic, value=data)
        self.producer.flush()

    def close(self):
        self.producer.close()


def main():
    admin_client = SimpleClient('172.17.12.244:39092')
    admin_client.create_topic("test-topic")
    print(admin_client.list_topics())

    bootstrap_server = "172.17.12.244:39092"
    producer = SimpleProducer(bootstrap_server)
    producer.send("test-topic", "SimpleProducer")
    producer.close()

    consumer = SimpleConsumer(
        "test-topic",
        bootstrap_servers="172.17.12.244:39092"
    )
    consumer.read()
    consumer.close()

    # admin_client.delete_topic("test-topic")
    # print(admin_client.list_topics())
    admin_client.close()


if __name__ == "__main__":
    main()
