from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')  # Backend non-interactif pour Docker
import matplotlib.pyplot as plt
import os

# Configuration
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "postgres",
    "database": "cryptowatch_db",
    "user": "airflow",
    "password": "airflow"
}

REPORT_DIR = "/opt/airflow/reports"

# -----------------------------
# Fonctions utilitaires
# -----------------------------

def get_db_connection():
    """Crée une connexion PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)

def log_to_monitoring(dag_id, task_id, execution_date, status, duration, error_message=None):
    """Insère un log dans pipeline_monitoring"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_monitoring 
            (dag_id, task_id, execution_date, status, duration, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (dag_id, task_id, execution_date, status, duration, error_message))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Erreur lors du log monitoring : {e}")

# -----------------------------
# Tâches du DAG
# -----------------------------

def check_data_availability(**context):
    """
    Vérifie qu'il y a suffisamment de données pour la date d'exécution
    Le TD demande : lever une exception si < 1000 enregistrements
    """
    start_time = datetime.now()
    # TEMPORAIRE : Utiliser aujourd'hui pour tester (normalement on utilise context['ds'])
    execution_date = datetime.now().strftime('%Y-%m-%d')  # AUJOURD'HUI
    # execution_date = context['ds']  # À réactiver plus tard
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    # Seuil réduit pour tester (normalement 1000)
    MIN_RECORDS = 50  # Au lieu de 1000
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Compter les enregistrements pour hier
        cur.execute("""
            SELECT COUNT(*) FROM crypto_prices 
            WHERE DATE(timestamp) = %s
        """, (execution_date,))
        count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        duration = int((datetime.now() - start_time).total_seconds())
        
        if count < MIN_RECORDS:
            error_msg = f"Données insuffisantes : {count} < {MIN_RECORDS} enregistrements"
            logger.error(f"❌ {error_msg}")
            log_to_monitoring(dag_id, task_id, execution_date, "FAILED", duration, error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"✅ {count} enregistrements trouvés pour {execution_date}")
        log_to_monitoring(dag_id, task_id, execution_date, "SUCCESS", duration)
        
        # Passer le count au contexte pour les tâches suivantes
        context['ti'].xcom_push(key='record_count', value=count)
        
    except Exception as e:
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "FAILED", duration, str(e))
        raise

def compute_daily_statistics(**context):
    """
    Calcule les statistiques quotidiennes pour chaque crypto
    - Prix moyen, min, max
    - Volume total
    - Volatilité (écart-type des prix)
    - Top 10 cryptos par performance
    """
    start_time = datetime.now()
    # TEMPORAIRE : Utiliser aujourd'hui pour tester
    execution_date = datetime.now().strftime('%Y-%m-%d')  # AUJOURD'HUI
    # execution_date = context['ds']  # À réactiver plus tard
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    try:
        conn = get_db_connection()
        
        # Récupérer les données de la journée
        query = """
            SELECT 
                crypto_id,
                symbol,
                price_usd,
                volume_24h,
                price_change_24h,
                timestamp
            FROM crypto_prices
            WHERE DATE(timestamp) = %s
            ORDER BY crypto_id, timestamp
        """
        df = pd.read_sql(query, conn, params=[execution_date])
        
        if df.empty:
            raise ValueError(f"Aucune donnée à traiter pour {execution_date}")
        
        logger.info(f"📊 Traitement de {len(df)} enregistrements")
        
        # Calcul des statistiques par crypto
        stats = df.groupby('crypto_id').agg(
            avg_price=('price_usd', 'mean'),
            min_price=('price_usd', 'min'),
            max_price=('price_usd', 'max'),
            total_volume=('volume_24h', 'sum'),
            volatility=('price_usd', 'std')  # Écart-type des PRIX (pas price_change_24h)
        ).reset_index()
        
        # Arrondir les valeurs
        stats['avg_price'] = stats['avg_price'].round(8)
        stats['min_price'] = stats['min_price'].round(8)
        stats['max_price'] = stats['max_price'].round(8)
        stats['volatility'] = stats['volatility'].round(2)
        
        # Insertion dans daily_crypto_stats
        cur = conn.cursor()
        insert_data = []
        for _, row in stats.iterrows():
            insert_data.append((
                execution_date,
                row['crypto_id'],
                float(row['avg_price']),
                float(row['min_price']),
                float(row['max_price']),
                int(row['total_volume']) if pd.notnull(row['total_volume']) else 0,
                float(row['volatility']) if pd.notnull(row['volatility']) else 0
            ))
        
        insert_query = """
            INSERT INTO daily_crypto_stats 
            (date, crypto_id, avg_price, min_price, max_price, total_volume, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, crypto_id) DO UPDATE
            SET avg_price = EXCLUDED.avg_price,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                total_volume = EXCLUDED.total_volume,
                volatility = EXCLUDED.volatility
        """
        execute_batch(cur, insert_query, insert_data)
        conn.commit()
        
        logger.info(f"💾 {len(stats)} stats quotidiennes insérées pour {execution_date}")
        
        # Identifier le Top 10 par performance
        top_performers = df.groupby('crypto_id')['price_change_24h'].mean().nlargest(10)
        logger.info(f"🏆 Top 10 cryptos : {top_performers.to_dict()}")
        
        # Passer les stats au contexte
        context['ti'].xcom_push(key='stats_count', value=len(stats))
        context['ti'].xcom_push(key='top_performers', value=top_performers.to_dict())
        
        cur.close()
        conn.close()
        
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "SUCCESS", duration)
        
    except Exception as e:
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "FAILED", duration, str(e))
        raise

def analyze_market_trends(**context):
    """
    Analyse les tendances du marché :
    - Cryptos en hausse continue (3+ jours)
    - Corrélation Bitcoin vs Altcoins
    - RSI simplifié
    - Moyennes mobiles
    """
    start_time = datetime.now()
    # TEMPORAIRE : Utiliser aujourd'hui pour tester
    execution_date = datetime.now().strftime('%Y-%m-%d')  # AUJOURD'HUI
    # execution_date = context['ds']  # À réactiver plus tard
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    try:
        conn = get_db_connection()
        
        # Récupérer les données des 7 derniers jours
        query = """
            SELECT date, crypto_id, avg_price, volatility
            FROM daily_crypto_stats
            WHERE date >= %s::date - INTERVAL '6 days'
            ORDER BY crypto_id, date
        """
        df = pd.read_sql(query, conn, params=[execution_date])
        
        if df.empty:
            logger.warning("⚠️ Pas assez de données historiques pour l'analyse")
            conn.close()
            return
        
        # 1. Cryptos en hausse continue (3+ jours)
        rising_cryptos = []
        for crypto_id in df['crypto_id'].unique():
            crypto_df = df[df['crypto_id'] == crypto_id].sort_values('date')
            if len(crypto_df) >= 3:
                prices = crypto_df['avg_price'].values[-3:]
                if all(prices[i] < prices[i+1] for i in range(len(prices)-1)):
                    rising_cryptos.append(crypto_id)
        
        logger.info(f"📈 Cryptos en hausse continue (3+ jours) : {rising_cryptos}")
        
        # 2. Corrélation Bitcoin vs autres cryptos
        bitcoin_df = df[df['crypto_id'] == 'bitcoin'][['date', 'avg_price']].rename(
            columns={'avg_price': 'btc_price'}
        )
        
        correlations = {}
        for crypto_id in df['crypto_id'].unique():
            if crypto_id != 'bitcoin':
                crypto_df = df[df['crypto_id'] == crypto_id][['date', 'avg_price']]
                merged = pd.merge(bitcoin_df, crypto_df, on='date')
                if len(merged) >= 2:
                    corr = merged['btc_price'].corr(merged['avg_price'])
                    correlations[crypto_id] = round(corr, 3) if pd.notnull(corr) else 0
        
        logger.info(f"🔗 Corrélations avec Bitcoin : {correlations}")
        
        # 3. RSI simplifié (Relative Strength Index)
        # RSI = 100 - (100 / (1 + RS)) où RS = moyenne gains / moyenne pertes
        rsi_values = {}
        for crypto_id in df['crypto_id'].unique():
            crypto_df = df[df['crypto_id'] == crypto_id].sort_values('date')
            if len(crypto_df) >= 7:
                prices = crypto_df['avg_price'].values
                changes = np.diff(prices)
                gains = changes[changes > 0].mean() if len(changes[changes > 0]) > 0 else 0
                losses = abs(changes[changes < 0].mean()) if len(changes[changes < 0]) > 0 else 0.01
                rs = gains / losses if losses != 0 else 0
                rsi = 100 - (100 / (1 + rs))
                rsi_values[crypto_id] = round(rsi, 2)
        
        logger.info(f"📊 RSI (7 jours) : {rsi_values}")
        
        # Passer les analyses au contexte
        context['ti'].xcom_push(key='rising_cryptos', value=rising_cryptos)
        context['ti'].xcom_push(key='correlations', value=correlations)
        context['ti'].xcom_push(key='rsi_values', value=rsi_values)
        
        conn.close()
        
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "SUCCESS", duration)
        
    except Exception as e:
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "FAILED", duration, str(e))
        raise

def generate_market_report(**context):
    """
    Génère un rapport quotidien avec :
    - CSV avec métriques clés
    - Graphiques matplotlib
    - Export vers volume partagé
    """
    start_time = datetime.now()
    # TEMPORAIRE : Utiliser aujourd'hui pour tester
    execution_date = datetime.now().strftime('%Y-%m-%d')  # AUJOURD'HUI
    # execution_date = context['ds']  # À réactiver plus tard
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    try:
        # Créer le répertoire reports si nécessaire
        os.makedirs(REPORT_DIR, exist_ok=True)
        
        conn = get_db_connection()
        
        # 1. Générer le CSV
        query = """
            SELECT * FROM daily_crypto_stats
            WHERE date = %s
            ORDER BY total_volume DESC
        """
        df = pd.read_sql(query, conn, params=[execution_date])
        
        csv_path = f"{REPORT_DIR}/crypto_report_{execution_date}.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"📄 CSV généré : {csv_path}")
        
        # 2. Générer les graphiques
        if not df.empty:
            # Graphique 1 : Top 10 par volume
            top_10_volume = df.nlargest(10, 'total_volume')
            
            plt.figure(figsize=(12, 6))
            plt.bar(top_10_volume['crypto_id'], top_10_volume['total_volume'])
            plt.xlabel('Crypto')
            plt.ylabel('Volume 24h (USD)')
            plt.title(f'Top 10 Cryptos par Volume - {execution_date}')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            volume_chart = f"{REPORT_DIR}/volume_chart_{execution_date}.png"
            plt.savefig(volume_chart)
            plt.close()
            logger.info(f"📊 Graphique volume généré : {volume_chart}")
            
            # Graphique 2 : Prix moyens Top 10
            top_10_price = df.nlargest(10, 'avg_price')
            
            plt.figure(figsize=(12, 6))
            plt.bar(top_10_price['crypto_id'], top_10_price['avg_price'])
            plt.xlabel('Crypto')
            plt.ylabel('Prix moyen (USD)')
            plt.title(f'Top 10 Cryptos par Prix - {execution_date}')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            price_chart = f"{REPORT_DIR}/price_chart_{execution_date}.png"
            plt.savefig(price_chart)
            plt.close()
            logger.info(f"📊 Graphique prix généré : {price_chart}")
            
            # Graphique 3 : Volatilité
            top_volatile = df.nlargest(10, 'volatility')
            
            plt.figure(figsize=(12, 6))
            plt.bar(top_volatile['crypto_id'], top_volatile['volatility'], color='red', alpha=0.7)
            plt.xlabel('Crypto')
            plt.ylabel('Volatilité (écart-type)')
            plt.title(f'Top 10 Cryptos par Volatilité - {execution_date}')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            volatility_chart = f"{REPORT_DIR}/volatility_chart_{execution_date}.png"
            plt.savefig(volatility_chart)
            plt.close()
            logger.info(f"📊 Graphique volatilité généré : {volatility_chart}")
        
        # 3. Résumé du rapport
        record_count = context['ti'].xcom_pull(task_ids='check_data', key='record_count')
        stats_count = context['ti'].xcom_pull(task_ids='compute_daily_statistics', key='stats_count')
        rising_cryptos = context['ti'].xcom_pull(task_ids='analyze_market_trends', key='rising_cryptos')
        
        summary = f"""
=== RAPPORT QUOTIDIEN CRYPTO ===
Date : {execution_date}
Enregistrements traités : {record_count}
Cryptos analysées : {stats_count}
Cryptos en hausse continue : {len(rising_cryptos) if rising_cryptos else 0}

Fichiers générés :
- {csv_path}
- {volume_chart if not df.empty else 'N/A'}
- {price_chart if not df.empty else 'N/A'}
- {volatility_chart if not df.empty else 'N/A'}

Top 5 cryptos par volume :
{df.nlargest(5, 'total_volume')[['crypto_id', 'total_volume']].to_string(index=False)}
================================
"""
        
        summary_path = f"{REPORT_DIR}/summary_{execution_date}.txt"
        with open(summary_path, 'w') as f:
            f.write(summary)
        
        logger.info(summary)
        
        conn.close()
        
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "SUCCESS", duration)
        
    except Exception as e:
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "FAILED", duration, str(e))
        raise

def send_report_notification(**context):
    """
    Envoie une notification de succès avec les métriques principales
    (Simplifié : log les infos, mais pourrait envoyer un email)
    """
    start_time = datetime.now()
    # TEMPORAIRE : Utiliser aujourd'hui pour tester
    execution_date = datetime.now().strftime('%Y-%m-%d')  # AUJOURD'HUI
    # execution_date = context['ds']  # À réactiver plus tard
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    
    try:
        stats_count = context['ti'].xcom_pull(task_ids='compute_daily_statistics', key='stats_count')
        rising_cryptos = context['ti'].xcom_pull(task_ids='analyze_market_trends', key='rising_cryptos')
        top_performers = context['ti'].xcom_pull(task_ids='compute_daily_statistics', key='top_performers')
        
        notification = f"""
🎉 Pipeline quotidien terminé avec succès !
📅 Date : {execution_date}
📊 {stats_count} cryptos analysées
📈 {len(rising_cryptos) if rising_cryptos else 0} en hausse continue
🏆 Top performer : {list(top_performers.keys())[0] if top_performers else 'N/A'}
"""
        
        logger.info(notification)
        
        # Ici on pourrait envoyer un email avec SendGrid, ou un message Slack
        # from airflow.operators.email import EmailOperator
        # ou utiliser un webhook Slack
        
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "SUCCESS", duration)
        
    except Exception as e:
        duration = int((datetime.now() - start_time).total_seconds())
        log_to_monitoring(dag_id, task_id, execution_date, "FAILED", duration, str(e))
        raise

# -----------------------------
# Définition du DAG
# -----------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=8),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(hours=2),
}

with DAG(
    'daily_crypto_analysis_test',  # ← NOUVEAU NOM ICI
    default_args=default_args,
    description='[TEST] Pipeline quotidien d\'analyse crypto - Calcul des stats et génération de rapports',
    schedule='0 1 * * *',
    start_date=pendulum.today('UTC').add(days=-2),
    catchup=False,
    tags=['crypto', 'daily', 'etl', 'test']  # ← Ajout du tag 'test'
) as dag:
    
    # Tâche 1 : Vérifier la disponibilité des données
    check_data = PythonOperator(
        task_id='check_data',
        python_callable=check_data_availability,
    )
    
    # Tâche 2 : Calculer les statistiques quotidiennes
    compute_stats = PythonOperator(
        task_id='compute_daily_statistics',
        python_callable=compute_daily_statistics,
    )
    
    # Tâche 3 : Analyser les tendances du marché
    analyze_trends = PythonOperator(
        task_id='analyze_market_trends',
        python_callable=analyze_market_trends,
    )
    
    # Tâche 4 : Générer le rapport
    generate_report = PythonOperator(
        task_id='generate_market_report',
        python_callable=generate_market_report,
    )
    
    # Tâche 5 : Envoyer la notification
    send_notification = PythonOperator(
        task_id='send_report_notification',
        python_callable=send_report_notification,
    )
    
    # Définition des dépendances
    check_data >> compute_stats >> analyze_trends >> generate_report >> send_notification