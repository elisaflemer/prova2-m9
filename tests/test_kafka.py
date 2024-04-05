from kafka import producer, consumer
from confluent_kafka import Producer, Consumer, KafkaError
import json
from pymongo import MongoClient

test_message = {
    "idSensor": "sensor_001",
    "timestamp": "2024-04-04T12:34:56Z",
    "tipoPoluente": "PM2.5",
    "nivel": 35.2
}

def test_integrity():
    test_producer = producer.create_producer()
    test_consumer = consumer.create_consumer('test_topic')
    producer.publish_message(test_producer, 'test_topic', test_message)
    msg = test_consumer.poll(timeout=1.0)
    decoded = msg.value().decode('utf-8')
    decoded = decoded.replace("'", "\"")
    assert json.loads(decoded) == test_message

def test_persistence():
    test_producer = producer.create_producer()
    test_consumer = consumer.create_consumer('test_topic')
    producer.publish_message(test_producer, 'test_topic', test_message)
    msg = test_consumer.poll(timeout=1.0)
    decoded = msg.value().decode('utf-8')
    decoded = decoded.replace("'", "\"")
    consumer.handle_message(decoded)

    client = MongoClient('mongodb://localhost:27017/')
    db = client['meu_banco']
    colecao = db['minha_colecao']
    assert colecao.find_one(test_message) == test_message