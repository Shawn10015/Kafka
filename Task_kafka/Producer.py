from confluent_kafka import Producer
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor

#settings, connect to Kafka
producer = Producer({
        'bootstrap.servers': 'localhost:9092'
})

#create some sensors
sensor = [
    {"type": "temperature", "name": "temperature_sensor_1", "frequency": 2},
    {"type": "temperature", "name": "temperature_sensor_2", "frequency": 3},
    {"type": "temperature", "name": "temperature_sensor_3", "frequency": 4},
    {"type": "temperature", "name": "temperature_sensor_4", "frequency": 5},
    {"type": "pressure", "name": "pressure_sensor_1", "frequency": 10},
    {"type": "pressure", "name": "pressure_sensor_2", "frequency": 8},
    {"type": "pressure", "name": "pressure_sensor_3", "frequency": 15},
    {"type": "light", "name": "light_sensor_1", "frequency": 9},
    {"type": "light", "name": "light_sensor_2", "frequency": 3},
]

#init sensor
def sensor_data(sensor_type, sensor_name):
    if sensor_type == "temperature":
        value = random.uniform(12, 30)
    elif sensor_type == "pressure":
        value = random.uniform(1, 10)
    else:
        value = random.uniform(0, 1)   

    return {
        'time': int(time.time()),
        'sensor_type': sensor_type,
        'sensor_name': sensor_name,
        'value': value
    }

#send single sensor
def producer_send(sensor):
    while True:
        data  = sensor_data(sensor["type"], sensor["name"])
        producer.produce('iot_topic', json.dumps(data))
        producer.flush()
        time.sleep(sensor["frequency"])

#automaticlly independently running
with ThreadPoolExecutor(max_workers=len(sensor)) as sensor_thread:
    for single_sensor in sensor:
        sensor_thread.submit(producer_send, single_sensor)