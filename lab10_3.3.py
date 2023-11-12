
from kafka import KafkaConsumer
from kafka import TopicPartition
import json


consumer = KafkaConsumer(bootstrap_servers='lab9.alumchat.fun')
consumer.assign([TopicPartition('201011', 0)])

for message in consumer:
    print(json.loads(message.value.decode('utf-8')))
