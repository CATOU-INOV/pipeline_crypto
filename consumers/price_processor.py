#!/usr/bin/env python3
"""
Consommateur Kafka pour le traitement des données crypto
Fonctionnalités :
- Validation des messages
- Enrichissement (volatilité, high_volatility flag)
- Détection d'alertes
- Insertion batch en base de données
- Gestion du Dead Letter Queue
- Nettoyage des valeurs numériques pour respecter les types DECIMAL/BIGINT du TD
"""

import json
import time
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging
from decimal import Decimal, ROUND_DOWN

# Configuration logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration Kafka
KAFKA_BROKER = "kafka:29092"
TOPIC_INPUT = "crypto-prices"
TOPIC_ALERTS = "crypto-alerts"
TOPIC_DLQ = "dead-letter-queue"
CONSUMER_GROUP = "crypto-consumer-group"

# Configuration PostgreSQL
DB_CONFIG = {
    "host": "postgres",
    "dbname": "cryptowatch_db",
    "user": "airflow",
    "password": "airflow"
}

# Configuration du traitement par batch
BATCH_SIZE = 50
BATCH_TIMEOUT = 10  # secondes

# Seuils d'alertes
ALERT_THRESHOLDS = {
    "bitcoin_price": 50000,  # USD
    "high_volatility": 10,   # %
    "extreme_drop": 15,      # %
    "high_volume": 50_000_000_000  # USD
}


# -------------------
# Fonctions utilitaires
# -------------------
def sanitize_numeric(value, max_digits=18, decimal_places=8):
    """Convertit une valeur en Decimal arrondi pour respecter DECIMAL/NUMERIC du TD"""
    try:
        d = Decimal(str(value))
        quantize_str = f"1.{'0'*decimal_places}"
        d = d.quantize(Decimal(quantize_str), rounding=ROUND_DOWN)
        if d.adjusted() > max_digits - decimal_places - 1:
            return Decimal(0)
        return d
    except:
        return Decimal(0)


class CryptoProcessor:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.db_conn = None
        self.batch = []
        self.last_batch_time = time.time()
        self.previous_prices = {}  # Pour calculer la volatilité

    # -------------------
    # Kafka
    # -------------------
    def init_kafka_consumer(self):
        """Initialise le consumer Kafka avec retry"""
        while True:
            try:
                self.consumer = KafkaConsumer(
                    TOPIC_INPUT,
                    bootstrap_servers=[KAFKA_BROKER],
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    group_id=CONSUMER_GROUP,
                    max_poll_records=BATCH_SIZE
                )
                logger.info("✅ Kafka Consumer connecté")
                return
            except NoBrokersAvailable:
                logger.warning("⏳ Kafka non disponible, retry dans 5s...")
                time.sleep(5)

    def init_kafka_producer(self):
        """Initialise le producer Kafka pour les alertes"""
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("✅ Kafka Producer connecté")
                return
            except NoBrokersAvailable:
                logger.warning("⏳ Producer Kafka non disponible, retry dans 5s...")
                time.sleep(5)

    # -------------------
    # PostgreSQL
    # -------------------
    def init_database(self):
        """Connexion PostgreSQL avec retry"""
        while True:
            try:
                self.db_conn = psycopg2.connect(**DB_CONFIG)
                logger.info("✅ Connexion PostgreSQL réussie")
                self.create_tables()
                return
            except Exception as e:
                logger.error(f"❌ Erreur connexion PostgreSQL : {e}")
                time.sleep(5)

    def create_tables(self):
        """Vérifie que les tables existent (créées par init.sql)"""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'crypto_prices'
                    );
                """)
                exists = cur.fetchone()[0]
                if exists:
                    logger.info("🗄️  Tables vérifiées (créées par init.sql)")
                else:
                    logger.error("❌ Table crypto_prices n'existe pas - init.sql non exécuté ?")
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification des tables : {e}")

    # -------------------
    # Validation / Enrichissement
    # -------------------
    def validate_message(self, data):
        required_fields = ["crypto_id", "price_usd", "timestamp"]
        for field in required_fields:
            if field not in data:
                return False, f"Champ manquant: {field}"
        try:
            price = float(data["price_usd"])
            if price <= 0:
                return False, f"Prix invalide: {price}"
        except (ValueError, TypeError):
            return False, f"Prix non numérique: {data['price_usd']}"
        if not isinstance(data, dict):
            return False, "Structure JSON invalide"
        return True, None

    def enrich_data(self, data):
        crypto_id = data["crypto_id"]
        current_price = float(data["price_usd"])
        data["processing_timestamp"] = datetime.now().isoformat()
        if crypto_id in self.previous_prices:
            previous_price = self.previous_prices[crypto_id]
            volatility = abs((current_price - previous_price) / previous_price * 100)
            data["volatility"] = round(volatility, 2)
            data["high_volatility"] = volatility > ALERT_THRESHOLDS["high_volatility"]
        else:
            data["volatility"] = None
            data["high_volatility"] = False
        self.previous_prices[crypto_id] = current_price
        return data

    def detect_alerts(self, data):
        alerts = []
        crypto_id = data["crypto_id"]
        price = float(data["price_usd"])
        if crypto_id == "bitcoin" and price > ALERT_THRESHOLDS["bitcoin_price"]:
            alerts.append({
                "crypto_id": crypto_id,
                "alert_type": "price_threshold",
                "threshold_value": ALERT_THRESHOLDS["bitcoin_price"],
                "current_value": price,
                "message": f"🚨 Bitcoin a franchi {ALERT_THRESHOLDS['bitcoin_price']} USD (prix actuel: {price} USD)",
                "triggered_at": datetime.now().isoformat()
            })
        price_change_24h = data.get("price_change_24h")
        if price_change_24h and float(price_change_24h) < -ALERT_THRESHOLDS["extreme_drop"]:
            alerts.append({
                "crypto_id": crypto_id,
                "alert_type": "extreme_drop",
                "threshold_value": -ALERT_THRESHOLDS["extreme_drop"],
                "current_value": price_change_24h,
                "message": f"⚠️ {crypto_id} a perdu {abs(price_change_24h):.2f}% en 24h",
                "triggered_at": datetime.now().isoformat()
            })
        volume_24h = data.get("volume_24h")
        if volume_24h and float(volume_24h) > ALERT_THRESHOLDS["high_volume"]:
            alerts.append({
                "crypto_id": crypto_id,
                "alert_type": "high_volume",
                "threshold_value": ALERT_THRESHOLDS["high_volume"],
                "current_value": volume_24h,
                "message": f"📊 Volume exceptionnellement élevé pour {crypto_id} : {volume_24h:,.0f} USD",
                "triggered_at": datetime.now().isoformat()
            })
        if data.get("high_volatility"):
            alerts.append({
                "crypto_id": crypto_id,
                "alert_type": "high_volatility",
                "threshold_value": ALERT_THRESHOLDS["high_volatility"],
                "current_value": data.get("volatility"),
                "message": f"🔥 Forte volatilité détectée pour {crypto_id} : {data.get('volatility'):.2f}%",
                "triggered_at": datetime.now().isoformat()
            })
        for alert in alerts:
            self.producer.send(TOPIC_ALERTS, alert)
            logger.info(f"🚨 {alert['message']}")
        return alerts

    def send_to_dlq(self, message, error):
        dlq_message = {
            "original_message": message,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "topic": TOPIC_INPUT
        }
        self.producer.send(TOPIC_DLQ, dlq_message)
        logger.warning(f"⚠️ Message envoyé au DLQ : {error}")

    # -------------------
    # Batch & insertion
    # -------------------
    def insert_batch(self):
        if not self.batch:
            return
        try:
            with self.db_conn.cursor() as cur:
                insert_query = """
                    INSERT INTO crypto_prices 
                    (crypto_id, symbol, price_usd, market_cap, volume_24h, 
                     price_change_24h, timestamp, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
                batch_data = []
                for item in self.batch:
                    batch_data.append((
                        item.get("crypto_id"),
                        item.get("symbol"),
                        sanitize_numeric(item.get("price_usd")),
                        int(item.get("market_cap") or 0),
                        int(item.get("volume_24h") or 0),
                        sanitize_numeric(item.get("price_change_24h"), max_digits=10, decimal_places=2),
                        item.get("timestamp"),
                        item.get("source", "coingecko")
                    ))
                execute_batch(cur, insert_query, batch_data)

                alerts_to_insert = []
                for item in self.batch:
                    if item.get("alerts"):
                        for alert in item["alerts"]:
                            alerts_to_insert.append((
                                alert["crypto_id"],
                                alert["alert_type"],
                                sanitize_numeric(alert.get("threshold_value")),
                                sanitize_numeric(alert.get("current_value")),
                                alert.get("triggered_at"),
                                alert["message"]
                            ))
                if alerts_to_insert:
                    alert_query = """
                        INSERT INTO price_alerts 
                        (crypto_id, alert_type, threshold_value, current_value, 
                         triggered_at, message)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    execute_batch(cur, alert_query, alerts_to_insert)

                self.db_conn.commit()
                logger.info(f"💾 Batch de {len(self.batch)} messages insérés en base")

            self.batch = []
            self.last_batch_time = time.time()

        except Exception as e:
            logger.error(f"❌ Erreur lors de l'insertion batch : {e}")
            self.db_conn.rollback()

    def should_flush_batch(self):
        batch_full = len(self.batch) >= BATCH_SIZE
        timeout_reached = (time.time() - self.last_batch_time) >= BATCH_TIMEOUT
        return batch_full or timeout_reached

    # -------------------
    # Traitement message
    # -------------------
    def process_message(self, message):
        data = message.value
        is_valid, error = self.validate_message(data)
        if not is_valid:
            self.send_to_dlq(data, error)
            return
        data = self.enrich_data(data)
        alerts = self.detect_alerts(data)
        data["alerts"] = alerts
        self.batch.append(data)
        if self.should_flush_batch():
            self.insert_batch()

    # -------------------
    # Main
    # -------------------
    def run(self):
        self.init_kafka_consumer()
        self.init_kafka_producer()
        self.init_database()

        logger.info("🚀 Price Processor démarré, en attente de messages...")

        try:
            for message in self.consumer:
                self.process_message(message)
                self.consumer.commit()
        except KeyboardInterrupt:
            logger.info("⏹️  Arrêt demandé par l'utilisateur")
        except Exception as e:
            logger.error(f"❌ Erreur fatale : {e}")
        finally:
            if self.batch:
                self.insert_batch()
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            if self.db_conn:
                self.db_conn.close()
            logger.info("👋 Price Processor arrêté proprement")


if __name__ == "__main__":
    processor = CryptoProcessor()
    processor.run()
