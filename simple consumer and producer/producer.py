from pykafka import KafkaClient

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    client = KafkaClient(hosts='127.0.0.1:9092')
    topic = client.topics[b'stream_topic']
    producer = topic.get_producer()
    producer.produce(b'Hello, Streaming!')
    producer.produce(b'Kafka first Message!')
    producer.stop()