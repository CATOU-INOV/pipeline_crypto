#!/usr/bin/env python3
"""
Crypto Producer - Envoi des données CoinGecko vers Kafka
et insertion dans PostgreSQL
"""

import os
import json
import time
import logging
import requests
import psycopg2
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

# -----------------------------------
# Logging
# -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger(__name__)

# -----------------------------------
# Variables d'environnement
# -----------------------------------
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
INTERVAL = int(os.getenv("API_POLL_INTERVAL", 30))
CRYPTOS = ["bitcoin", "ethereum", "dogecoin"]

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "cryptowatch_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

# -----------------------------------
# Kafka
# -----------------------------------
def init_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
            )
            logger.info(f"✅ Kafka connecté sur {BROKER}")
            return producer
        except NoBrokersAvailable:
            logger.warning("⚠️ Kafka non disponible, nouvel essai dans 5 secondes...")
            time.sleep(5)

# -----------------------------------
# PostgreSQL
# -----------------------------------
def init_postgres():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        logger.info(f"✅ PostgreSQL connecté sur {POSTGRES_HOST}")
        return conn
    except Exception as e:
        logger.error(f"❌ Erreur connexion PostgreSQL : {e}")
        raise

def insert_prices(conn, data, timestamp):
    """Insère les données dans la table crypto_prices"""
    with conn.cursor() as cur:
        for crypto, values in data.items():
            cur.execute(
                """
                INSERT INTO crypto_prices (crypto_id, price_usd, timestamp)
                VALUES (%s, %s, %s)
                """,
                (crypto, values["usd"], timestamp)
            )
    conn.commit()
    logger.info("💾 Données insérées dans PostgreSQL")

# -----------------------------------
# CoinGecko
# -----------------------------------
def fetch_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ",".join(CRYPTOS), "vs_currencies": "usd"}
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"📈 Données récupérées : {data}")
        return data
    except requests.RequestException as e:
        logger.error(f"❌ Erreur API CoinGecko : {e}")
        return None

# -----------------------------------
# Main
# -----------------------------------
def main():
    logger.info("🚀 Démarrage du producteur crypto")
    producer = init_producer()
    conn = init_postgres()

    while True:
        data = fetch_crypto_prices()
        if data:
            timestamp = datetime.utcnow().isoformat()
            message = {
                "timestamp": timestamp,
                "data": data,
                "source": "coingecko"
            }

            # --- Kafka ---
            try:
                producer.send(TOPIC, value=message)
                producer.flush()
                logger.info(f"📤 Message envoyé vers Kafka : {message}")
            except KafkaError as e:
                logger.error(f"❌ Erreur d’envoi Kafka : {e}")

            # --- PostgreSQL ---
            try:
                insert_prices(conn, data, timestamp)
            except Exception as e:
                logger.error(f"❌ Erreur insertion PostgreSQL : {e}")
        else:
            logger.warning("⚠️ Aucune donnée envoyée (API indisponible)")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt du producteur (interruption utilisateur)")
