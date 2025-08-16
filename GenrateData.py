import time
import json
import random
import paho.mqtt.client as mqtt

BROKER = "localhost"  # or "mqtt-broker" if running from inside docker
PORT = 1883
TOPIC_STATE = "lab/esp32/esp32-lab-01/state"
TOPIC_SENSORS = "lab/esp32/esp32-lab-01/sensors"

client = mqtt.Client()

client.connect(BROKER, PORT, 60)
print(f"Connected to MQTT broker at {BROKER}:{PORT}")

try:
    while True:
        # Publish "online" state
        client.publish(TOPIC_STATE, "online")

        # Simulate sensor payload
        payload = {
            "dhtA": {"ok": True, "t": round(random.uniform(20, 30), 1), "h": round(random.uniform(40, 60), 1)},
            "dhtB": {"ok": True, "t": round(random.uniform(20, 30), 1), "h": round(random.uniform(40, 60), 1)},
            "acc":  {"x": round(random.uniform(-2, 2), 2), "y": round(random.uniform(-2, 2), 2), "z": round(random.uniform(-2, 2), 2)}
        }

        client.publish(TOPIC_SENSORS, json.dumps(payload))
        print(f"Published to {TOPIC_SENSORS}: {payload}")

        time.sleep(2)

except KeyboardInterrupt:
    print("Stopped publishing.")
    client.publish(TOPIC_STATE, "offline")
    client.disconnect()
