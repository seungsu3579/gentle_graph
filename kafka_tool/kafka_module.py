import os
import sys
import yaml
from json import dumps
from json import loads
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition


class Producer:
    def __init__(self, config_file: str, value_type: str = "json", acks: int = 1):

        with open(config_file) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)

        hosts = conf["kafka"]["hostname"]
        port = conf["kafka"]["port"]

        if value_type == "json":
            self.producer = KafkaProducer(
                bootstrap_servers=[
                    f"{hosts[0]}:{port}",
                    f"{hosts[1]}:{port}",
                    f"{hosts[2]}:{port}",
                ],
                acks=acks,
                compression_type="gzip",
                api_version=(0, 11, 5),
                value_serializer=lambda x: dumps(x).encode("utf-8"),
            )

        elif value_type == "string":
            self.producer = KafkaProducer(
                bootstrap_servers=[
                    f"{hosts[0]}:{port}",
                    f"{hosts[1]}:{port}",
                    f"{hosts[2]}:{port}",
                ],
                acks=acks,
                compression_type="gzip",
                api_version=(0, 11, 5),
                value_serializer=lambda x: x.encode("utf-8"),
            )

            # key 를 넣을떈 아래 조건도 KafkaProducer에 추가
            # key_serializer=lambda x: x.encode("utf-8"),

    def __del__(self):
        self.producer.close()

    def send_to_topic(self, topic: str, value):
        self.producer.send(topic=topic, value=value).add_callback(
            self.on_send_success
        )
        # .add_callback(self.on_send_error)
        self.producer.flush()

    def on_send_success(self, metadata):
        print(
            "topic:",
            metadata.topic,
            "partition:",
            metadata.partition,
            "offset:",
            metadata.offset,
        )

    def on_send_error(self, excp):

        print(excp)


class Consumer:
    def __init__(self, config_file: str, group_id: str, value_type: str = "json"):

        with open(config_file) as f:
            conf = yaml.load(f, Loader=yaml.FullLoader)

        hosts = conf["kafka"]["hostname"]
        port = conf["kafka"]["port"]

        if value_type == "json":
            self.consumer = KafkaConsumer(
                bootstrap_servers=[
                    f"{hosts[0]}:{port}",
                    f"{hosts[1]}:{port}",
                    f"{hosts[2]}:{port}",
                ],
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: loads(x.decode("utf-8")),
                consumer_timeout_ms=1000,
            )

        elif value_type == "string":
            self.consumer = KafkaConsumer(
                bootstrap_servers=[
                    f"{hosts[0]}:{port}",
                    f"{hosts[1]}:{port}",
                    f"{hosts[2]}:{port}",
                ],
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda x: x.decode("utf-8"),
                consumer_timeout_ms=1000,
            )

    def get_data(self, topic: str, reset_offset=False):

        if reset_offset:
            tp = TopicPartition(topic, 0)
            partitions = [tp]
        else:
            PARTITIONS = []
            for partition in self.consumer.partitions_for_topic(topic):
                PARTITIONS.append(TopicPartition(topic, partition))
            partitions = self.consumer.end_offsets(PARTITIONS)

        self.consumer.assign(partitions)
        
        self.consumer.poll(max_records=10, update_offsets=True)
        data = []
        for msg in self.consumer:
            data.append(msg)

        self.consumer.commit()
        print(f"receive data num : {len(data)}")
        return data
