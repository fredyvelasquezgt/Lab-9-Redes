# Javier Valle
# Angel Higueros
# Fredy Velasquez

from kafka import KafkaProducer
import random
import matplotlib.pyplot as plt
import json


def generate_random_number_gaussian():
    return int(random.gauss(50, 20))


def generate_random_float_number_gaussian():
    return random.gauss(50, 20)


def get_random_cardinal_point():
    cardinal_points = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return cardinal_points[random.randint(0, 7)]


def get_random_values():
    return {"temperature": generate_random_float_number_gaussian(), "humidity": generate_random_number_gaussian(), "wind_direction": get_random_cardinal_point()}


producer = KafkaProducer(bootstrap_servers='lab9.alumchat.xyz')

for i in range(10):
    x = get_random_values()
    print('Mensaje enviado: ', x)
    producer.send('201011', json.dumps(x).encode('utf-8'))
    plt.pause(5)
