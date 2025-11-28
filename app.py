import os
import time
import threading
from flask import Flask
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision

# ---------------------- ENV VARIABLES ----------------------
MQTT_BROKER = "d797988ab08841459e366cdaa3ab7482.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "hivemq.webclient.1764152586161"
MQTT_PASS = "0,w3&<A%asX4b8CLNkqR"
MQTT_TOPIC = "lorawan/data"

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

# ---------------------- INFLUXDB CLIENT ----------------------
influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx.write_api(write_options=None)

# ---------------------- MQTT CALLBACKS ----------------------
def on_message(client, userdata, msg):
    payload = msg.payload.decode()

    print("📩 MQTT:", payload)

    try:
        parts = payload.split(",")
        temp = float(parts[0].split(":")[1].replace("°C", ""))
        hum = float(parts[1].split(":")[1].replace("%", ""))
        pres = float(parts[2].split(":")[1].replace(" hPa", ""))

        point = (
            Point("bme280_data")
            .tag("device", "esp32_lora")
            .field("temperature", temp)
            .field("humidity", hum)
            .field("pressure", pres)
            .time(time.time_ns(), WritePrecision.NS)
        )

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        print("📤 Stored to InfluxDB")

    except Exception as e:
        print("❌ Parse error:", e)


def mqtt_thread():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()  # Required by HiveMQ Cloud

    client.on_message = on_message

    print("🔌 Connecting to HiveMQ...")
    client.connect(MQTT_BROKER, MQTT_PORT)

    client.subscribe(MQTT_TOPIC)
    print("🔊 Subscribed to:", MQTT_TOPIC)

    client.loop_forever()


# ---------------------- FLASK APP (Render requirement) ----------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "MQTT → InfluxDB Bridge Running Successfully!"

# ---------------------- START EVERYTHING ----------------------
if __name__ == "__main__":
    # Start MQTT subscriber in background thread
    t = threading.Thread(target=mqtt_thread)
    t.daemon = True
    t.start()

    # Start Flask web server so Render can bind PORT
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
