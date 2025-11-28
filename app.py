import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime, timedelta, timezone

# ------------------------------------------
# InfluxDB Cloud Configuration
# ------------------------------------------
influx_url = "https://us-east-1-1.aws.cloud2.influxdata.com"
influx_token = "oQAvjIjAcDenTgi-nE79kuUNszk9Sr70uaf_VLDTKrHFo_lTE_JC6FNDOLFkaGycdDW9zM5MVQgLvlflH10qDg=="
influx_org = "cloudburst"
influx_bucket = "cloudburst_data"

client_influx = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
write_api = client_influx.write_api()

# ------------------------------------------
# HiveMQ Cloud Configuration
# ------------------------------------------
mqtt_host = "d797988ab08841459e366cdaa3ab7482.s1.eu.hivemq.cloud"
mqtt_port = 8883
mqtt_user = "hivemq.webclient.1764152586161"
mqtt_pass = "0,w3&<A%asX4b8CLNkqR"

# ------------------------------------------
# Proper IST timezone-aware timestamp
# ------------------------------------------
IST = timezone(timedelta(hours=5, minutes=30))

def ist_time():
    return datetime.now(IST)

# ------------------------------------------
# When a new MQTT message arrives
# ------------------------------------------
def on_message(client, userdata, msg):
    payload = msg.payload.decode()

    try:
        value = float(payload)
        point = (
            Point("mqtt_lorawan")
            .tag("topic", msg.topic)
            .field("value", value)
            .time(ist_time(), WritePrecision.NS)
        )
    except:
        point = (
            Point("mqtt_lorawan")
            .tag("topic", msg.topic)
            .field("value", payload)
            .time(ist_time(), WritePrecision.NS)
        )

    write_api.write(bucket=influx_bucket, org=influx_org, record=point)
    print(f"[SAVED] {msg.topic} => {payload} @ IST: {ist_time()}")

# ------------------------------------------
# Connect to HiveMQ Cloud
# ------------------------------------------
client = mqtt.Client()
client.username_pw_set(mqtt_user, mqtt_pass)
client.tls_set()
client.on_message = on_message

print("Connecting to HiveMQ Cloud...")
client.connect(mqtt_host, mqtt_port)

client.subscribe("lorawan/data")
print("Listening for messages on 'lorawan/data'...")
client.loop_forever()
