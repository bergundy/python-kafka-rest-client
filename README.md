# Kafka REST Client
Convenience wrapper for Kafka REST proxy

## Intallation
* TODO: upload to pypi
```bash
pip install git+https://github.com/bergundy/python-kafka-rest-client
```

### Producer Example
```python
from kafka_rest_client import JsonProducer

producer = JsonProducer()

for x in range(10):
    producer.produce('my_topic', str(x))
```

### Consumer Example
```python
from kafka_rest_client import Client

client = Client()

message_format = 'json'  # or 'avro' / 'binary'

with client.create_consumer('example_group', message_format, offset_reset='smallest') as consumer:
    for m in consumer.messages('my_topic'):
        print m['key'], m['value']
```

### Avro Producer
```python

from kafka_rest_client import AvroProducer

key_schema = { ... }
value_schema = { ... }

producer = AvroProducer(hosts, key_schema=key_schema, value_schema=value_schema)
producer.produce('avro_topic', {'key': { ... }, 'value': { ... })
```
