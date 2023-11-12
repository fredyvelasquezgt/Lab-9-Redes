from kafka import KafkaProducer, KafkaConsumer
import json
import random
import time
import matplotlib.pyplot as plt

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

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='lab9.alumchat.xyz:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Función para enviar datos al topic de Kafka
def enviar_datos_al_kafka():
    data = generar_data()
    producer.send('201011', value=data)

# Función para procesar el mensaje
def procesar_mensaje(mensaje):
    return json.loads(mensaje.value)

# Función para graficar los datos en vivo
def plot_live_data(temperaturas, humedades, direcciones_viento):
    plt.ion()  # Activar modo interactivo
    fig, axs = plt.subplots(3, 1, figsize=(10, 8))

    while True:
        # Graficando temperaturas
        axs[0].plot(temperaturas, label='Temperaturas')
        axs[0].set_ylabel('Temperatura')

        # Graficando humedades
        axs[1].plot(humedades, label='Humedades', color='orange')
        axs[1].set_ylabel('Humedad')

        # Graficando direcciones de viento
        axs[2].bar(range(len(direcciones_viento)), direcciones_viento, color='green', alpha=0.7)
        axs[2].set_ylabel('Dirección de Viento')
        axs[2].set_xticks(range(len(direcciones_viento)))
        axs[2].set_xticklabels(direcciones_viento)

        # Mostrando la leyenda en el primer subgráfico
        axs[0].legend()

        plt.tight_layout()

        plt.pause(1)  # Pausa de 1 segundo antes de la próxima actualización

# Configuración del consumidor de Kafka
consumer = KafkaConsumer('201011', group_id='15', bootstrap_servers='lab9.alumchat.xyz:9092', enable_auto_commit=True, auto_offset_reset='latest')

# Listas para almacenar datos
all_temp = []
all_hume = []
all_wind = []

# Tiempo inicial
start_time = time.time()

# Bucle para consumir mensajes durante 10 segundos.
while (time.time() - start_time) <= 10:
    # Hacer un poll para obtener mensajes nuevos
    mensajes = consumer.poll(timeout_ms=15000)  # Consumir cada 1 segundo

    # Procesar mensajes recibidos
    for tp, mensajes_tp in mensajes.items():
        for mensaje in mensajes_tp:
            payload = procesar_mensaje(mensaje)

            # Almacenando datos en listas
            all_temp.append(payload['temperatura'])
            all_hume.append(payload['humedad'])
            all_wind.append(payload['direccion_viento'])

            # Enviando nuevos datos al topic cada segundo
            enviar_datos_al_kafka()
            time.sleep(1)

            tiempo_transcurrido = time.time() - start_time
            print("Tiempo transcurrido: ", tiempo_transcurrido)

# Graficando los datos al finalizar el bucle
plot_live_data(all_temp, all_hume, all_wind)
