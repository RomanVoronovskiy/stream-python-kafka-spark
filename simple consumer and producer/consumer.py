from pykafka import KafkaClient

if __name__ == "__main__":
    client = KafkaClient(hosts='127.0.0.1:9092')
    topic = client.topics[b'stream_topic']
    consumer = topic.get_simple_consumer()
    for message in consumer:
        if message is not None:
            print(message.value.decode('utf-8'))
