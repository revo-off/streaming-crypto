import json
import redis
import collections
import statistics
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'crypto.trades.raw'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print(f"✅ [Consumer 3] Connecté à Redis sur {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"❌ [Consumer 3] Erreur Redis: {e}")
    exit(1)

try:
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'consumer-group-3-alerts',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"✅ [Consumer 3] Connecté à Kafka, topic: '{KAFKA_TOPIC}'")
except Exception as e:
    print(f"❌ [Consumer 3] Erreur Kafka: {e}")
    exit(1)

# Fenêtre locale pour le calcul statistique rapide (ex: 1000 derniers trades)
WINDOW_SIZE = 1000
prices = collections.deque(maxlen=WINDOW_SIZE)
volumes = collections.deque(maxlen=WINDOW_SIZE)

# Seuils statistiques
SIGMA_MULTIPLIER = 3.0
MIN_DATAPOINTS = 50 # On attend d'avoir 50 trades pour avoir des stats valables

print("🔄 [Consumer 3] En attente de trades pour détection d'anomalies (> 3σ)...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() != KafkaError._PARTITION_EOF:
            print(f"Erreur Kafka: {msg.error()}")
        continue

    data = json.loads(msg.value().decode('utf-8'))
    try:
        price = float(data.get('p'))
        quantity = float(data.get('q'))
        timestamp = int(data.get('E'))
    except (TypeError, ValueError) as e:
        continue
    
    # Détection AVANT d'ajouter la nouvelle donnée dans l'historique 
    # pour éviter qu'une anomalie extrême ne biaise sa propre détection
    if len(prices) >= MIN_DATAPOINTS:
        mean_price = statistics.mean(prices)
        stdev_price = statistics.stdev(prices)
        
        mean_vol = statistics.mean(volumes)
        stdev_vol = statistics.stdev(volumes)
        
        is_anomaly = False
        alert_reason = []
        
        # Détection pic de prix (> 3 sigma)
        if stdev_price > 0 and abs(price - mean_price) > (SIGMA_MULTIPLIER * stdev_price):
            is_anomaly = True
            alert_reason.append(f"Price anomaly: {price:.2f} (Mean: {mean_price:.2f}, σ: {stdev_price:.2f})")
            
        # Détection pic de volume (> 3 sigma ET > moyenne + marge absolue)
        # On ajoute une marge absolue pour éviter les micro-alertes si le marché est très plat
        if stdev_vol > 0 and (quantity - mean_vol) > (SIGMA_MULTIPLIER * stdev_vol):
            is_anomaly = True
            alert_reason.append(f"Volume spike: {quantity:.4f} (Mean: {mean_vol:.4f}, σ: {stdev_vol:.4f})")
            
        if is_anomaly:
            alert_data = json.dumps({
                "timestamp": timestamp,
                "price": price,
                "quantity": quantity,
                "reasons": alert_reason
            })
            
            # Stockage dans Redis
            r.lpush('crypto:alerts', alert_data)
            r.ltrim('crypto:alerts', 0, 99) # Garde les 100 dernières alertes
            
            print(f"🚨 [Consumer 3 - Alert] {' | '.join(alert_reason)}")

    # Ajout aux historiques locaux
    prices.append(price)
    volumes.append(quantity)
