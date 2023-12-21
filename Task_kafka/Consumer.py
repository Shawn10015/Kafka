from confluent_kafka import Consumer, KafkaException
import json
import pandas as pd
import time

#connect to Kafka
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'iot_consumer',
    'auto.offset.reset': 'latest'
})

consumer.subscribe(['iot_topic'])
#init dataframe
df = pd.DataFrame(columns=['time', 'sensor_type', 'sensor_name', 'value'])

last_current_time = time.time()

try:
    while True: 
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())
        else:
            sensor_data = json.loads(message.value().decode('utf-8'))
            df = pd.concat([df, pd.DataFrame([sensor_data])], ignore_index=True)
            # print(f"Receive data: {sensor_data}")

        current_time = time.time()

        #every 20s, unix timestamp cannot accurately at 20.000s
        if 20 <= current_time - last_current_time < 22:
            mean_sensor_type = df.groupby('sensor_type')['value'].mean().to_dict()
            mean_sensor_name = df.groupby('sensor_name')['value'].mean().to_dict()
            
            print(f"Average of Sensor By Type: {mean_sensor_type}")
            print(f"Average of Sensor By Name: {mean_sensor_name}")

            df = df.iloc[0:0]

            last_current_time = current_time

finally:
    consumer.close()