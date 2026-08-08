
from kafka import KafkaConsumer
import snowflake.connector
import json

consumer = KafkaConsumer(
    "container.telemetry",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

conn = snowflake.connector.connect(
    user="HADIAKTHAR",
    password="Asiyasali30000",
    account="VRENGKD-VT61292",
    warehouse="ATMOSYNC_WH",
    database="ATMOSYNC_DB",
    schema="RAW"
)

cursor = conn.cursor()

for message in consumer:
    data = message.value

    cursor.execute("""
        INSERT INTO RAW_TELEMETRY
        VALUES (%s,%s,%s,%s,%s)
    """,
    (
        data["container_id"],
        data["timestamp"],
        data["temperature"],
        data["humidity"],
        data["vibration"]
    ))

    conn.commit()

    print("Inserted:", data)



    