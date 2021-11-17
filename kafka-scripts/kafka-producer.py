from time import sleep
from json import dumps
from faker import Faker
from kafka import KafkaProducer
import json
fake = Faker()
import time

def get_registered_data():
    return {
        'first name': fake.first_name(),
        'last name': fake.last_name(),
        'age': fake.random_int(0, 60),
        'address': fake.address(),
        'register year': fake.year(),
        'register month': fake.month(),
        'register day': fake.day_of_month(),
        'monthly income ($NT)': fake.random_int(28000, 100000)
    }



producer = KafkaProducer(bootstrap_servers=['192.168.29.72:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


for e in range(1000):
    registered_data = get_registered_data()
    print(registered_data)
    producer.send('my_topic', registered_data).get(timeout=10)
    sleep(5)