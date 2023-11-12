from kafka import KafkaProducer
import random
import matplotlib.pyplot as plt
import json
import sys


def generate_random_number_gaussian():
    return int(random.gauss(50, 20))


def generate_random_float_number_gaussian():
    return random.gauss(50, 20)


def get_random_cardinal_point():
    cardinal_points = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return cardinal_points[random.randint(0, 7)]


def get_random_values():
    return {"temperature": generate_random_float_number_gaussian(), "humidity": generate_random_number_gaussian(), "wind_direction": get_random_cardinal_point()}


def int_to_binary(number):
    return bin(number)[2:].zfill(7)


def string_to_binary(string):
    return ''.join(format(ord(x), 'b') for x in string)


def separate_decimal(number):
    integer = int(number)
    decimal = int((number - integer) * 100)
    return integer, decimal


def convert_to_binary(temperature, humidity, wind_direction):
    print(temperature, humidity, wind_direction)
    temperature_integer, temperature_decimal = separate_decimal(temperature)
    temperature_integer = int_to_binary(temperature_integer)
    temperature_decimal = int_to_binary(temperature_decimal)
    temperature = temperature_integer + temperature_decimal

    humidity = int_to_binary(humidity)

    cardinal_points = {"N": "000", "NE": "001", "E": "010",
                       "SE": "011", "S": "100", "SW": "101", "W": "110", "NW": "111"}
    wind_direction = cardinal_points[wind_direction]

    print(len(temperature), len(humidity), len(wind_direction))
    print(temperature, humidity, wind_direction)

    return temperature + humidity + wind_direction


producer = KafkaProducer(bootstrap_servers='lab9.alumchat.xyz')

for i in range(3):
    data = get_random_values()
    x = convert_to_binary(data["temperature"],
                          data["humidity"], data["wind_direction"])
    print("mensaje original: ", data)
    print('Mensaje enviado: ', x)
    producer.send('201011', json.dumps(x).encode('utf-8'))
    plt.pause(5)
