import paho.mqtt.client as mqtt
import time
import random
import json

broker = "localhost"
port = 1883
topic = "sensor/dht11"

# MQTT client setup
client = mqtt.Client(client_id="PythonSimulator")
client.connect(broker, port, keepalive=60)

def generate_normal_value(low, high):
    return round(random.uniform(low, high), 2)

def generate_anomaly_value(low, high, anomaly_range):
    """Randomly generate a normal or anomalous value."""
    if random.random() < 0.1:  # 10% chance to generate anomaly
        # Anomalous values outside normal range
        if random.random() < 0.5:
            return round(low - random.uniform(*anomaly_range), 2)
        else:
            return round(high + random.uniform(*anomaly_range), 2)
    return generate_normal_value(low, high)

try:
    while True:
        # Simulate sensor readings with possible anomalies
        temperature = generate_anomaly_value(20.0, 35.0, (5, 15))
        humidity = generate_anomaly_value(30.0, 80.0, (10, 30))
        vibration_x = generate_anomaly_value(0.0, 1.0, (1.0, 3.0))
        vibration_y = generate_anomaly_value(0.0, 1.0, (1.0, 3.0))
        vibration_z = generate_anomaly_value(0.0, 1.0, (1.0, 3.0))
        current = generate_anomaly_value(5.0, 10.0, (2.0, 5.0))

        payload = {
            "temperature": temperature,
            "humidity": humidity,
            "vibration": {
                "x": vibration_x,
                "y": vibration_y,
                "z": vibration_z
            },
            "current": current
        }

        client.publish(topic, json.dumps(payload))
        print(f"Published: {payload} to topic '{topic}'")

        time.sleep(5)

except KeyboardInterrupt:
    print("Simulation stopped.")
    client.disconnect()
