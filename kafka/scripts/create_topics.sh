#!/bin/bash

# Adresse du broker Kafka (interne au Docker)
BROKER=kafka:29092

# Attendre que Kafka soit prêt
echo "Attente du broker Kafka..."
while ! kafka-topics --bootstrap-server $BROKER --list &>/dev/null; do
  echo "Kafka non disponible, attente..."
  sleep 2
done

echo "Création des topics Kafka..."

# Topic prix en temps réel : 3 partitions, replication factor 1
kafka-topics --create --topic crypto-prices --partitions 3 --replication-factor 1 --if-not-exists --bootstrap-server $BROKER

# Topic données traitées : 2 partitions
kafka-topics --create --topic crypto-processed --partitions 2 --replication-factor 1 --if-not-exists --bootstrap-server $BROKER

# Topic alertes prix : 1 partition
kafka-topics --create --topic crypto-alerts --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server $BROKER

# Dead-letter queue : 1 partition
kafka-topics --create --topic dead-letter-queue --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server $BROKER

echo "Tous les topics ont été créés !"
