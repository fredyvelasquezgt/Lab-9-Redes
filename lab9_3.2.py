from confluent_kafka import Producer
import json
import random
import time

# Configuración del Kafka Producer
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic = '201011'  # Reemplaza con tu número de carné
producer_config = {'bootstrap.servers': bootstrap_servers}

# Crear el productor
producer = Producer(producer_config)

# Funciones para generar datos
def generate_random_number_gaussian():
    return int(random.gauss(50, 20))

def generate_random_float_number_gaussian():
    return random.gauss(50, 20)

def get_random_cardinal_point():
    cardinal_points = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return cardinal_points[random.randint(0, 7)]

def generar_data():
    temperatura = generate_random_float_number_gaussian()
    humedad = generate_random_float_number_gaussian()
    direccion_viento = get_random_cardinal_point()

    data = {
        'temperatura': temperatura,
        'humedad': humedad,
        'direccion_viento': direccion_viento
    }

    return data

# Bucle principal del productor
try:
    while True:
        # Generar datos
        data = generar_data()

        # Imprimir los datos antes de enviarlos al topic
        print("Datos a enviar:", data)

        # Convertir datos a JSON
        data_json = json.dumps(data)

        # Enviar datos al topic
        result = producer.produce(topic=topic, value=data_json)

        # Esperar entre 15 y 30 segundos antes de enviar el próximo dato
        time.sleep(random.uniform(15, 30))

        # Esperar la respuesta del servidor y mostrarla
        msg = producer.poll(1)
        
        print("Respuesta del servidor:", msg)
            

except KeyboardInterrupt:
    print("Interrupción del teclado. Deteniendo el productor.")
finally:
    # Limpiar y cerrar el productor
    producer.flush()
