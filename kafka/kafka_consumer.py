import sys
sys.path.insert(0, '..')

from confluent_kafka import DeserializingConsumer,KafkaError,KafkaException
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from opensearch_client.opensearch_client import save_to_opensearch
import json
import socket
import sys
import logging

sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

avro_deserializer = AvroDeserializer(schema_registry_client)
string_deserializer = StringDeserializer('utf_8')

conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'opensearch',
    'auto.offset.reset': 'smallest',
    'enable.auto.commit': False,
    'key.deserializer': string_deserializer,
    'value.deserializer': avro_deserializer
}

consumer = DeserializingConsumer(conf)
poll = True

def poll(topics):
    try:
        count=0
        records=[]
        consumer.subscribe(topics)

        while poll:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
                continue
            
            if msg.value() is not None:
                records.append(msg.value())
                count += 1
            
            if count % 10 == 0:
                save_to_opensearch(records)
                records=[]
                # consumer.commit(asynchronous=False)
    finally:
        pass

def shutdown():
    poll = False

try:
    poll(['tweets'])
except BaseException as e:
    logging.error('Error!', exc_info=e)
    print('Stop')
    consumer.close()
    sys.exit(1)