#!/usr/bin/env python3
import json
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BROKER = "kafka:29092"
TOPIC = "crypto-prices"

DB_CONFIG = {
    "host": "postgres",
    "dbname": "cryptowatch_db",
    "user": "airflow",
    "password": "airflow"
}

# Initialiser Kafka Consumer avec retry
def init_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BROKER],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id="crypto-consumer-group"
            )
            print("✅ Kafka Consumer connecté.")
            return consumer
        except NoBrokersAvailable:
            print("Kafka non dispo, nouvelle tentative dans 5 secondes...")
            time.sleep(5)

# Connexion PostgreSQL
def init_db():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("✅ Connexion PostgreSQL réussie.")
            return conn
        except Exception as e:
            print(f"Erreur connexion PostgreSQL : {e}")
            time.sleep(5)

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                crypto VARCHAR(50),
                usd_price FLOAT
            );
        """)
        conn.commit()
        print("🧱 Table crypto_prices prête.")

def main():
    consumer = init_consumer()
    conn = init_db()
    create_table(conn)

    print("🚀 Price Processor démarré, en attente de messages...")
    for message in consumer:
        data = message.value
        timestamp = data.get("timestamp")
        prices = data.get("data", {})

        with conn.cursor() as cur:
            for crypto, info in prices.items():
                usd_price = info.get("usd")
                cur.execute("""
                    INSERT INTO crypto_prices (timestamp, crypto, usd_price)
                    VALUES (%s, %s, %s)
                """, (timestamp, crypto, usd_price))
            conn.commit()

        print(f"💾 Données insérées dans PostgreSQL : {data}")

if __name__ == "__main__":
    main()
