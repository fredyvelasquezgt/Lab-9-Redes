import json
import time
from random import uniform, randint, choice
from confluent_kafka import Producer
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Configuración del servidor y tópico Kafka
server_address = '157.245.244.105:9092'
data_topic = '201011'

# Configuración inicial para simulación de datos meteorológicos
temp_base = 50.0
humidity_base = 50
temp_variance = 10.0
humidity_variance = 10

# Configuración para el productor Kafka
kafka_config = {'bootstrap.servers': server_address}
data_producer = Producer(kafka_config)

def simulate_weather_data():
    temp = round(uniform(temp_base - temp_variance, temp_base + temp_variance), 1)
    humidity = randint(humidity_base - humidity_variance, humidity_base + humidity_variance)
    wind_dir = choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])
    return temp, humidity, wind_dir

def serialize_data(temp, humidity, wind_dir):
    wind_dir_map = {"N": 0, "NW": 1, "W": 2, "SW": 3, "S": 4, "SE": 5, "E": 6, "NE": 7}
    temp = int(((max(min(temp, 50), -50) + 50) * 163.83))
    wind_dir_encoded = wind_dir_map[wind_dir]
    data = (temp << 10) | (humidity << 3) | wind_dir_encoded
    return data.to_bytes(3, byteorder='big')

def deserialize_data(data_bytes):
    data_int = int.from_bytes(data_bytes, byteorder='big')
    temp = ((data_int >> 10) & 0x3FFF) / 163.84 - 50
    humidity = (data_int >> 3) & 0x7F
    wind_dir_num = data_int & 0x07
    wind_dir = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"][wind_dir_num]
    return temp, humidity, wind_dir

def send_weather_data():
    while True:
        temp, humidity, wind_dir = simulate_weather_data()
        encoded_data = serialize_data(temp, humidity, wind_dir)
        data_producer.produce(data_topic, value=encoded_data)
        data_producer.flush()
        time.sleep(30)

# Iniciar productor en un hilo
Thread(target=send_weather_data).start()

# Configurar consumidor
weather_consumer = KafkaConsumer(data_topic, group_id='weather_group', bootstrap_servers=[server_address])

# Función para graficar datos en tiempo real
def draw_plots(temp_list, humidity_list, wind_dir_list):
    plt.ion()
    fig, (ax_temp, ax_humidity, ax_wind) = plt.subplots(3, 1, figsize=(12, 9))
    
    # Actualizar gráfico de temperatura
    ax_temp.plot(temp_list, label='Temperature', color='red')
    ax_temp.set_title('Temperature over Time')
    ax_temp.set_xlabel('Time')
    ax_temp.set_ylabel('Temperature')
    ax_temp.legend()
    
    # Actualizar gráfico de humedad
    ax_humidity.plot(humidity_list, label='Humidity', color='blue')
    ax_humidity.set_title('Humidity over Time')
    ax_humidity.set_xlabel('Time')
    ax_humidity.set_ylabel('Humidity')
    ax_humidity.legend()
    
    # Actualizar gráfico de dirección de viento
    ax_wind.hist(wind_dir_list, bins=[0,1,2,3,4,5,6,7,8], align='left', color='green')
    ax_wind.set_xticks(range(8))
    ax_wind.set_xticklabels(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])
    ax_wind.set_title('Wind Direction Frequency')
    ax_wind.set_xlabel('Wind Direction')
    ax_wind.set_ylabel('Frequency')
    
    plt.tight_layout()
    plt.show()

# Listas para almacenar datos meteorológicos
temperature_data = []
humidity_data = []
wind_direction_data = []

# Procesar mensajes de Kafka y actualizar gráficos
for message in weather_consumer:
    if message.value:
        temp, humidity, wind_dir = deserialize_data(message.value)
        temperature_data.append(temp)
        humidity_data.append(humidity)
        wind_direction_data.append(wind_dir)
        draw_plots(temperature_data, humidity_data, wind_direction_data)

