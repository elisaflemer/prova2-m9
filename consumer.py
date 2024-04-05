from confluent_kafka import Producer, Consumer, KafkaError
from pymongo import MongoClient
import json

client = MongoClient('mongodb://localhost:27017/')
db = client['meu_banco']
colecao = db['minha_colecao']


# Configurações do produtor
producer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'client.id': 'python-producer'
}

# Configurações do consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

topic = 'qualidadeAr'

# Criar consumidor
consumer = Consumer(**consumer_config)

# Assinar tópico
consumer.subscribe([topic])

# Consumir mensagens
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'Received message: {msg.value().decode("utf-8")}')
        s = msg.value().decode('utf-8')
        s = s.replace("\'", "\"")
        colecao.insert_one(json.loads(s))
except KeyboardInterrupt:
    pass
finally:
    # Fechar consumidor
    consumer.close()