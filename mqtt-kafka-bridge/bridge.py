import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import json
import os

# Environment-based configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = "lab/esp32/#"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "iot-sensor-data"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        topic = msg.topic

        message = {
            "mqtt_topic": topic,
            "payload": json.loads(payload) if payload.startswith("{") else payload
        }

        producer.send(KAFKA_TOPIC, message)
        print(f"Sent to Kafka: {message}")

    except Exception as e:
        print(f"Error: {e}")

# Setup MQTT client
mqtt_client = mqtt.Client(client_id="MQTTKafkaBridge")
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

print("MQTT-to-Kafka bridge running...")
mqtt_client.loop_forever()
