import paho.mqtt.client as mqtt
import json

payload = {
    "timestamp": "2026-08-17T16:07:34Z",
    "engine": "crypto_sword",
    "status": "SCANNING",
    "top_regime_signal": {
        "ticker": "TESTING123",
        "regime": "Bull",
        "calculated_kelly": 0.0,
        "signal_strength": 0.0
    },
    "execution_payload": {
        "status": "SCANNING",
        "allocated_dollars": 0.0
    }
}

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.username_pw_set("tony", "phk_khv_fwg2jce7WQX")
client.connect("192.168.0.110", 1883, 60)
client.loop_start()
info = client.publish("mace/telemetry/crypto_sword", json.dumps(payload), retain=True)
info.wait_for_publish(timeout=5)
client.loop_stop()
client.disconnect()
print("Sent test payload!")
