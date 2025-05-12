import json
import time
import random
from awscrt.mqtt import Connection, Client, QoS
from awsiot import mqtt_connection_builder
import sys 

ENDPOINT = "a1jc42iddoh7sh-ats.iot.eu-north-1.amazonaws.com"  
CLIENT_ID = "sensor_001_producer" 
PATH_TO_CERTIFICATE = "certs/24f7539ea830969f325b21824a146db512ea47b63ca51af544f0a9027e38219e-certificate.pem.crt" 
PATH_TO_PRIVATE_KEY = "certs/24f7539ea830969f325b21824a146db512ea47b63ca51af544f0a9027e38219e-private.pem.key" 
PATH_TO_AMAZON_ROOT_CA_1 = "certs/AmazonRootCA1.pem" 
TOPIC = "iot/sensor_data" 
DEVICE_ID = "sensor_001"

def generate_sensor_data():
    temperature = round(random.uniform(18.0, 28.0), 2)
    humidity = round(random.uniform(40.0, 65.0), 2)
    timestamp = int(time.time())

    data = {
        "deviceId": DEVICE_ID,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity,
        "status": "ok"
    }
    return data

def on_connection_interrupted(connection, error, **kwargs):
    print(f"Connection interrupted. error: {error}")

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print(f"Connection resumed. return_code: {return_code} session_present: {session_present}")

if __name__ == "__main__":
    print("AWS IoT Core'a bağlanmaya çalışılıyor...")
    try:
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=30
        )
    except Exception as e:
        print(f"MQTT bağlantı nesnesi oluşturulurken hata: {e}")
        sys.exit(1)

    print(f"Connecting to {ENDPOINT} with client ID '{CLIENT_ID}'...")
    connect_future = mqtt_connection.connect()

    try:
        connect_future.result()
        print("Bağlantı başarılı!")
    except Exception as e:
        print(f"Bağlantı hatası: {e}")
        sys.exit(1)

    print(f"'{TOPIC}' konusuna mesajlar gönderiliyor...")
    try:
        for i in range(10): 
            sensor_data = generate_sensor_data()
            message_json = json.dumps(sensor_data)
            
            print(f"Gönderilen mesaj ({i+1}): {message_json}")
            pub_future, _ = mqtt_connection.publish(
                topic=TOPIC,
                payload=message_json,
                qos=QoS.AT_LEAST_ONCE 
            )
            pub_future.result() 
            print(f"Mesaj ({i+1}) '{TOPIC}' konusuna başarıyla gönderildi.")
            time.sleep(2) 
    except Exception as e:
        print(f"Mesaj gönderilirken hata: {e}")
    finally:
        print("Bağlantı kesiliyor...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("Bağlantı kesildi.") 