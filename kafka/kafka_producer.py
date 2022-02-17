from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import socket

schema_str = """
    {
        "namespace": "ct.twitter.kafka",
        "name": "Tweets",
        "type": "record",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "text", "type": "string"}
        ]
    }
    """
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(schema_registry_client,
                                 schema_str)

conf = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': socket.gethostname(),
    'compression.type': 'snappy',
    'linger.ms': '20',
    'batch.size': f'{32*1024}',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}
producer = SerializingProducer(conf)

def print_out(err, msg):
    if not err:
        print(f'Sent: {str(msg.value())}')
    else:
        print(f'Error: {err}')

def send(topic, key, value):
    producer.produce(topic=topic, key=key ,value=value, on_delivery=print_out)
    producer.poll(1)