
from confluent_kafka import Producer, Consumer, KafkaError
import json
from confluent_kafka.admin import AdminClient, NewTopic
kafka_bootstrap_servers = 'host.docker.internal:9092'
kafka_topic = 'bikes'
def create_topic(bootstrap_servers, topic_name, partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
    topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=replication_factor)
    futures = admin_client.create_topics([topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created successfully.")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
create_topic(kafka_bootstrap_servers, kafka_topic, partitions=1, replication_factor=1)