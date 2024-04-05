from confluent_kafka import Producer, Consumer, KafkaError
import datetime
import random
import json
import time

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

topic = ''

def read_config(filename):
    with open(filename, 'r') as f:
        config = json.load(f)
        return config 

config = read_config('config.json')

def generate_message():
    message = {}
    for item in config['fields']:
        if config['fields'][item]['random']['type'] == 'integer':
            print('here')
            message[item] = random.randint(config['fields'][item]['random']['min'], config['fields'][item]['random']['max'])
        elif config['fields'][item]['random']['type'] == 'array':
            message[item] = random.choice(config['fields'][item]['random']['values'])
        elif config['fields'][item]['random']['type'] == 'timestamp':
            message[item] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return json.dumps(message)
while True:
    message = generate_message()
    producer.produce(config['topic'], message.encode('utf-8'), callback=delivery_callback)

    producer.flush()
    time.sleep(1)