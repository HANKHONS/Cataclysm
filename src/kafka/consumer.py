from confluent_kafka import Consumer
import json
import psycopg


conn=psycopg.connect(dbname="cataclysm",
                     host="localhost",
                     port=5433,
                     user="cataclysm",
                    password="78520")
cursor=conn.cursor()

cursor.execute("""
        CREATE TABLE IF NOT EXISTS crisis_events (
            event_id VARCHAR(50) PRIMARY KEY,
            timestamp TIMESTAMP,
            disaster_type VARCHAR(50),
            severity VARCHAR(20),
            location VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            text TEXT,
            source VARCHAR(50),
            rainfall_mm FLOAT,
            water_level_m FLOAT,
            wind_speed_kmh FLOAT,
            temperature_c FLOAT
        );
        """)
conn.commit()
print("Connected to postrgresql")

conf={
    'bootstrap.servers': "localhost:9094",
    'group.id': "crisis-dashboard-group",
     'auto.offset.reset': 'earliest'
}

consumer=Consumer(conf)

consumer.subscribe(['crisis-events'])
print("listening to topic crisis-events")

try:
    while True:
        msg=consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        event_data = json.loads(msg.value().decode('utf-8'))

        meta = event_data.get('metadata', {})
        cursor.execute("""
            INSERT INTO crisis_events (
                event_id, timestamp, disaster_type, severity, location,
                latitude, longitude, text, source,
                rainfall_mm, water_level_m, wind_speed_kmh, temperature_c
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """, (
            event_data['event_id'],
            event_data['timestamp'],
            event_data['disaster_type'],
            event_data['severity'],
            event_data['location'],
            event_data['latitude'],
            event_data['longitude'],
            event_data['text'],
            event_data['source'],
            meta.get('rainfall_mm'),
            meta.get('water_level_m'),
            meta.get('wind_speed_kmh'),
            meta.get('temperature_c')
        ))
        conn.commit()
        print(f"Saved {event_data['event_id']} to PostgreSQL")
        print(event_data)

        print(f"RECEIVED EVENT: {event_data['disaster_type']}  in {event_data['location']} at {event_data['timestamp']}")
        print(f"Severity: {event_data['severity']}")
        print(f"Details: {event_data['text']}")
        print("-" * 40)

except KeyboardInterrupt:
        print("Stopping consumer...")
finally:
    cursor.close()
    conn.close()
    consumer.close()

