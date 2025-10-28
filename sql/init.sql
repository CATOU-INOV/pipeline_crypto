-- Table des prix temps réel
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    crypto_id VARCHAR(50),
    symbol VARCHAR(10),
    price_usd DECIMAL(18, 8),
    market_cap BIGINT,
    volume_24h BIGINT,
    price_change_24h DECIMAL(10, 2),
    timestamp TIMESTAMP,
    source VARCHAR(50)
);

-- Table des agrégations quotidiennes
CREATE TABLE IF NOT EXISTS daily_crypto_stats (
    date DATE,
    crypto_id VARCHAR(50),
    avg_price DECIMAL(18, 8),
    min_price DECIMAL(18, 8),
    max_price DECIMAL(18, 8),
    total_volume BIGINT,
    volatility DECIMAL(10, 2),
    PRIMARY KEY (date, crypto_id)
);

-- Table de monitoring
CREATE TABLE IF NOT EXISTS pipeline_monitoring (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    status VARCHAR(20),
    duration INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Table des alertes
CREATE TABLE IF NOT EXISTS price_alerts (
    id SERIAL PRIMARY KEY,
    crypto_id VARCHAR(50),
    alert_type VARCHAR(20),
    threshold_value DECIMAL(18, 8),
    current_value DECIMAL(18, 8),
    triggered_at TIMESTAMP,
    message TEXT
);
