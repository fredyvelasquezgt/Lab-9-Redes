from kafka import KafkaConsumer
from kafka import TopicPartition
import json


def convert_from_binary(binary):
    temperature_integer = int(binary[0:7], 2)
    temperature_decimal = int(binary[7:14], 2)
    temperature = temperature_integer + temperature_decimal / 100

    humidity = int(binary[14:21], 2)

    cardinal_points = {"000": "N", "001": "NE", "010": "E",
                       "011": "SE", "100": "S", "101": "SW", "110": "W", "111": "NW"}
    wind_direction = cardinal_points[binary[21:24]]

    return temperature, humidity, wind_direction


consumer = KafkaConsumer(bootstrap_servers='lab9.alumchat.fun')
consumer.assign([TopicPartition('201011', 0)])

for message in consumer:
    print("Mensaje recibido: ", message.value.decode('utf-8'))
    print("Mensaje decifrado: ")
    # quit " " from message
    message = message.value.decode('utf-8')[1:-1]
    print(convert_from_binary(message))
