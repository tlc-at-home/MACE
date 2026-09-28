import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc, properties=None):
    client.subscribe("mace/telemetry/#")

def on_message(client, userdata, msg):
    print(f"TOPIC: {msg.topic}")
    print(f"PAYLOAD: {msg.payload.decode()}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set("tony", "phk_khv_fwg2jce7WQX")
client.on_connect = on_connect
client.on_message = on_message
client.connect("192.168.0.110", 1883, 60)
client.loop_start()
import time
time.sleep(2)
client.loop_stop()
client.disconnect()
