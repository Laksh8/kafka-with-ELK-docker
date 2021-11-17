from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
    'my_topic',
     bootstrap_servers=['192.168.29.72:29092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

# consumer.ensure_topic_exists('numtest')

for message in consumer:
    message = message.value
    print('{} Message'.format(message))