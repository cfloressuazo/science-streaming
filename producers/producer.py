"""Producer base-class providing common utilities and functionality"""
from utils.logger import Logger
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = Logger


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            'BROKER_URL': 'PLAINTEXT://localhost:9092',
            'SCHEMA_REGISTRY': 'http://localhost:8081',
            'KAFKA_REST_PROXY': 'http://localhost:8082'
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            {"bootstrap.servers": self.broker_properties['BROKER_URL'],
             "schema.registry.url": self.broker_properties['SCHEMA_REGISTRY']},
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    @staticmethod
    def topic_exists(client, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_name in set(t.topic for t in iter(topic_metadata.topics.values()))

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": self.broker_properties['BROKER_URL']})
        exists = self.topic_exists(client, self.topic_name)
        if exists is False:
            client.create_topics(
                [
                    NewTopic(
                        topic=self.topic_name,
                        num_partitions=self.num_partitions,
                        replication_factor=self.num_replicas
                    )
                ]
            )
            logger.info(f"Kafka topic {self.topic_name} has been created", class_name=self.__class__.__name__)
        else:
            logger.info(f"Kafka topic {self.topic_name} already exists", class_name=self.__class__.__name__)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
