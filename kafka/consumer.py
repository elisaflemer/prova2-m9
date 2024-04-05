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

consumer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

def create_consumer(topic):
    consumer = Consumer(**consumer_config)
    consumer.subscribe([topic])
    return consumer

def read_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
        return config 
    
def save_to_mongo(decoded):
    colecao.insert_one(json.loads(decoded))

config = read_config('config.json')

consumer = create_consumer(config['topic'])

def handle_message(decoded):
    save_to_mongo(decoded)
    display_formatted_message(decoded)

def display_formatted_message(decoded):
    dictionary = json.loads(decoded)

    console_format = config['console_format']
    msg = console_format.format(**dictionary)
    print(msg)
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
        decoded = msg.value().decode('utf-8')
        decoded = decoded.replace("'", "\"")
        handle_message(decoded)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()