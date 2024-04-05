from confluent_kafka import Producer, Consumer, KafkaError
import time
import random

# Configurações do produtor
producer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'client.id': 'python-producer'
}

# Criar produtor
producer = Producer(**producer_config)

# Função de callback para confirmação de entrega
def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Enviar mensagem
topic = 'qualidadeAr'

def generate_message():
    return str({
    "idSensor": random.choice(["sensor_001", "sensor_002", "sensor_003"]),
    "timestamp": int(time.time()),
    "tipoPoluente": random.choice(["PM2.5", "PM10", "CO", "NO2", "O3", "SO2"]),
    "nivel": random.randint(0, 100)
})
while True:
    message = generate_message()
    producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)

    # Aguardar a entrega de todas as mensagens
    producer.flush()
    time.sleep(1)