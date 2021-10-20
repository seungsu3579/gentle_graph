import os
import sys
import yaml
from json import loads
from kafka import KafkaConsumer
from kafka import TopicPartition


config_file = os.path.abspath(sys.argv[1])
with open(config_file) as f:
    conf = yaml.load(f, Loader=yaml.FullLoader)

hosts = conf["kafka"]["hostname"]
port = conf["kafka"]["port"]

consumer = KafkaConsumer(
    "daum_news",
    bootstrap_servers=[
        f"{hosts[0]}:{port}",
        f"{hosts[1]}:{port}",
        f"{hosts[2]}:{port}",
    ],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode("utf-8")),
    consumer_timeout_ms=1000,
)

for message in consumer:
    assert isinstance(message.value, dict)
    print(message)
    # print(
    #     f"Topic : {message.topic}, Offset : {message.offset}, Key : {message.key}, Value : {message.value.decode('utf-8')}"
    # )
