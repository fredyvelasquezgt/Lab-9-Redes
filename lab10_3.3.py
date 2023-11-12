from kafka import KafkaConsumer
import json
import time
import matplotlib.pyplot as plt

# Función para procesar el mensaje
def procesar_mensaje(mensaje):
    return json.loads(mensaje.value)

# Función para graficar los datos
def plot_all_data(temperaturas, humedades, direcciones_viento):
    # Crear una figura con 3 subgráficos
    fig, axs = plt.subplots(3, 1, figsize=(10, 8))

    # Graficar temperaturas
    axs[0].plot(temperaturas, label='Temperaturas')
    axs[0].set_ylabel('Temperatura')

    # Graficar humedades
    axs[1].plot(humedades, label='Humedades', color='orange')
    axs[1].set_ylabel('Humedad')

    # Graficar direcciones de viento
    axs[2].bar(range(len(direcciones_viento)), direcciones_viento, color='green', alpha=0.7)
    axs[2].set_ylabel('Dirección de Viento')
    axs[2].set_xticks(range(len(direcciones_viento)))
    axs[2].set_xticklabels(direcciones_viento)

    # Mostrar la leyenda en el primer subgráfico
    axs[0].legend()

    plt.tight_layout()

    # Mostrar la gráfica
    plt.show()

# Configuración del consumidor de Kafka
consumer = KafkaConsumer('201011', group_id='15', bootstrap_servers='lab9.alumchat.xyz:9092')

# Listas para almacenar datos
all_temp = []
all_hume = []
all_wind = []

# Bucle para consumir mensajes continuamente
for mensaje in consumer:
    # Procesar el mensaje
    payload = procesar_mensaje(mensaje)
    
    # Almacenar datos en listas
    all_temp.append(payload['temperatura'])
    all_hume.append(payload['humedad'])
    all_wind.append(payload['direccion_viento'])
    
    # Graficar los datos
    plot_all_data(all_temp, all_hume, all_wind)
