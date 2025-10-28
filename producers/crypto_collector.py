#!/usr/bin/env python3
"""
Crypto Producer - Envoi des données CoinGecko vers Kafka
CORRIGÉ : Utilise /coins/markets pour récupérer toutes les données
"""

import os
import json
import time
import logging
import requests
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
INTERVAL = int(os.getenv("API_POLL_INTERVAL", 60))  # 60s pour respecter rate limit
MAX_CRYPTOS = int(os.getenv("MAX_CRYPTOS", 50))

# Circuit Breaker
circuit_breaker_failures = 0
circuit_breaker_state = "CLOSED"  # CLOSED, OPEN
last_failure_time = None
CIRCUIT_BREAKER_THRESHOLD = 3
CIRCUIT_BREAKER_TIMEOUT = 120  # secondes

# -----------------------------------
# Kafka
# -----------------------------------
def init_producer():
    """Initialise le producteur Kafka avec retry"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BROKER],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",  # Attendre confirmation de tous les replicas
                retries=5,   # Retry automatique
                retry_backoff_ms=1000,  # Backoff exponentiel
                max_in_flight_requests_per_connection=1  # Garantir l'ordre
            )
            logger.info(f"✅ Kafka connecté sur {BROKER}")
            return producer
        except NoBrokersAvailable:
            logger.warning("⏳ Kafka non disponible, nouvel essai dans 5 secondes...")
            time.sleep(5)

# -----------------------------------
# CoinGecko API - BON ENDPOINT
# -----------------------------------
def fetch_crypto_data():
    """
    CHANGEMENT MAJEUR : Utilise /coins/markets au lieu de /simple/price
    Cet endpoint retourne TOUTES les données nécessaires
    """
    global circuit_breaker_failures, circuit_breaker_state, last_failure_time
    
    # Vérifier le circuit breaker
    if circuit_breaker_state == "OPEN":
        if time.time() - last_failure_time >= CIRCUIT_BREAKER_TIMEOUT:
            logger.info("🔄 Circuit breaker: Tentative de réouverture")
            circuit_breaker_state = "CLOSED"
            circuit_breaker_failures = 0
        else:
            logger.warning("⛔ Circuit breaker OUVERT - Appel API bloqué")
            return None
    
    # Endpoint corrigé : /coins/markets
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",  # Top cryptos par capitalisation
        "per_page": MAX_CRYPTOS,
        "page": 1,
        "sparkline": False,  # Pas besoin des graphiques
        "price_change_percentage": "24h"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Réinitialiser le circuit breaker en cas de succès
        circuit_breaker_failures = 0
        circuit_breaker_state = "CLOSED"
        
        logger.info(f"📡 Données récupérées : {len(data)} cryptomonnaies")
        return data
    
    except requests.exceptions.Timeout:
        logger.error("⏱️  Timeout lors de l'appel à l'API CoinGecko")
        circuit_breaker_failures += 1
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.error("🚫 Rate limit atteint sur l'API CoinGecko - Ralentir les appels")
        else:
            logger.error(f"❌ Erreur HTTP {e.response.status_code}: {e}")
        circuit_breaker_failures += 1
    
    except requests.RequestException as e:
        logger.error(f"❌ Erreur API CoinGecko : {e}")
        circuit_breaker_failures += 1
    
    # Gérer le circuit breaker
    if circuit_breaker_failures >= CIRCUIT_BREAKER_THRESHOLD:
        circuit_breaker_state = "OPEN"
        last_failure_time = time.time()
        logger.error(f"🔴 Circuit breaker OUVERT après {circuit_breaker_failures} échecs")
    
    return None

def format_message(crypto_data):
    """
    Transforme les données brutes de CoinGecko au format attendu
    Correspond au schéma de la table crypto_prices du TD
    """
    return {
        # Champs principaux (table crypto_prices)
        "crypto_id": crypto_data.get("id"),
        "symbol": crypto_data.get("symbol", "").upper(),
        "name": crypto_data.get("name"),
        "price_usd": crypto_data.get("current_price"),
        "market_cap": crypto_data.get("market_cap"),
        "volume_24h": crypto_data.get("total_volume"),
        "price_change_24h": crypto_data.get("price_change_percentage_24h"),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "coingecko",
        
        # Champs bonus pour analyses futures
        "high_24h": crypto_data.get("high_24h"),
        "low_24h": crypto_data.get("low_24h"),
        "circulating_supply": crypto_data.get("circulating_supply"),
        "total_supply": crypto_data.get("total_supply"),
        "max_supply": crypto_data.get("max_supply"),
        "market_cap_rank": crypto_data.get("market_cap_rank"),
        "ath": crypto_data.get("ath"),
        "ath_change_percentage": crypto_data.get("ath_change_percentage"),
        "ath_date": crypto_data.get("ath_date"),
        "atl": crypto_data.get("atl"),
        "atl_change_percentage": crypto_data.get("atl_change_percentage"),
        "atl_date": crypto_data.get("atl_date"),
        "last_updated": crypto_data.get("last_updated")
    }

def on_send_success(record_metadata):
    """Callback appelé lors du succès d'envoi"""
    logger.debug(
        f"✅ Message envoyé: topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, offset={record_metadata.offset}"
    )

def on_send_error(excp):
    """Callback appelé en cas d'erreur d'envoi"""
    logger.error(f"❌ Erreur d'envoi Kafka : {excp}")

# -----------------------------------
# Main
# -----------------------------------
def main():
    logger.info("🚀 Démarrage du producteur crypto (version corrigée)")
    logger.info(f"📊 Configuration : {MAX_CRYPTOS} cryptos, intervalle {INTERVAL}s")
    
    producer = init_producer()
    
    while True:
        start_time = time.time()
        
        # Récupérer les données (nouvel endpoint)
        crypto_data_list = fetch_crypto_data()
        
        if crypto_data_list:
            success_count = 0
            error_count = 0
            
            # Publier chaque crypto individuellement dans Kafka
            for crypto_data in crypto_data_list:
                try:
                    # Formater le message
                    message = format_message(crypto_data)
                    
                    # Utiliser crypto_id comme clé de partition pour garantir l'ordre
                    key = message["crypto_id"].encode('utf-8')
                    
                    # Envoi asynchrone avec callbacks
                    future = producer.send(TOPIC, key=key, value=message)
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                    
                    success_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Erreur pour {crypto_data.get('id')}: {e}")
                    error_count += 1
            
            # Attendre que tous les messages soient envoyés
            producer.flush()
            
            logger.info(
                f"📤 {success_count} messages envoyés à Kafka "
                f"({error_count} erreurs)"
            )
            
            # Exemple de log pour debug (montrer quelques cryptos)
            if success_count > 0:
                sample = crypto_data_list[:3]
                logger.info(
                    f"📈 Exemples: " +
                    ", ".join([
                        f"{c['name']}: ${c.get('current_price', 0):,.2f} "
                        f"({c.get('price_change_percentage_24h', 0):+.2f}%)"
                        for c in sample
                    ])
                )
        else:
            logger.warning("⚠️ Aucune donnée récupérée (API indisponible ou circuit breaker ouvert)")
        
        # Calculer le temps d'attente
        elapsed = time.time() - start_time
        sleep_time = max(0, INTERVAL - elapsed)
        
        if sleep_time > 0:
            logger.info(f"💤 Prochaine collecte dans {sleep_time:.0f}s...")
            time.sleep(sleep_time)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Arrêt du producteur (interruption utilisateur)")
    except Exception as e:
        logger.error(f"❌ Erreur fatale : {e}")
        raise