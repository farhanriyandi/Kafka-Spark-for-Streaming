# Method posts events to Kafka Server
# run command in kafka server to create topic :
# ./usr/bin/kafka-topics --create --topic device_data --bootstrap-server kafka:9092
import time
import random
import uuid

from weather_generate import generate_weather

from kafka import KafkaProducer
from kafka.errors import KafkaError

BROKER = "localhost:9092"
# BROKER = "kafka:19092"  # use this to run inside pyspark container


def post_to_kafka(data):
    print('data: '+ str(data))
    try:
        producer = KafkaProducer(bootstrap_servers=[BROKER], acks=1,)
        producer.send(
            'weather-data',
            key=bytes(str(uuid.uuid4()), 'utf-8'),
            value=data
        )
        print("Posted to topic")
    except KafkaError as e:
        print(f"Send failed: {e}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    try:
        while True:
            post_to_kafka(bytes(str(generate_weather()), 'utf-8'))
            time.sleep(random.randint(0, 5))
    except KeyboardInterrupt:
        print("\nProducer stopped.")