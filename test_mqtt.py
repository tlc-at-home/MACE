import paho.mqtt.client as mqtt
import json
import os

MQTT_BROKER_IP = "localhost" # Assuming it runs locally or we can use the same code as orchestrator

def on_connect(client, userdata, flags, rc, properties=None):
    client.subscribe("mace/telemetry/crypto")

def on_message(client, userdata, msg):
    print(msg.payload.decode())
    client.disconnect()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.connect("127.0.0.1", 1883, 60)
client.loop_forever()
