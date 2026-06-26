import json
import redis
from confluent_kafka import Consumer, KafkaError

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'crypto.trades.raw'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()
    print(f"✅ [Consumer 2] Connecté à Redis sur {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"❌ [Consumer 2] Erreur Redis: {e}")
    exit(1)

try:
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'consumer-group-2-agg',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"✅ [Consumer 2] Connecté à Kafka, topic: '{KAFKA_TOPIC}'")
except Exception as e:
    print(f"❌ [Consumer 2] Erreur Kafka: {e}")
    exit(1)

def calculate_aggregate(r_client, timestamp_ms, window_minutes):
    """Calcule le Volume Weighted Average Price (VWAP) et le volume total sur la fenêtre."""
    window_ms = window_minutes * 60 * 1000
    cutoff = timestamp_ms - window_ms
    
    trades = r_client.zrangebyscore('crypto:trades:agg_window', cutoff, timestamp_ms)
    
    total_volume = 0.0
    total_price_vol = 0.0
    
    for trade_str in trades:
        trade = json.loads(trade_str)
        p = trade['p']
        q = trade['q']
        total_volume += q
        total_price_vol += p * q
        
    vwap = total_price_vol / total_volume if total_volume > 0 else 0
    return vwap, total_volume

print("🔄 [Consumer 2] En attente de trades pour agrégations (1m, 5m, 15m, 1h)...")

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
    
    # 1. Stocker le trade dans la fenêtre glissante (max 1h)
    trade_data = json.dumps({"p": price, "q": quantity, "t": timestamp})
    r.zadd('crypto:trades:agg_window', {trade_data: timestamp})
    
    # 2. Nettoyer les données plus vieilles d'une heure (3600000 ms)
    cutoff_1h = timestamp - 3600000
    r.zremrangebyscore('crypto:trades:agg_window', '-inf', cutoff_1h)
    
    # 3. Calculer les agrégats
    agg_1m = calculate_aggregate(r, timestamp, 1)
    agg_5m = calculate_aggregate(r, timestamp, 5)
    agg_15m = calculate_aggregate(r, timestamp, 15)
    agg_1h = calculate_aggregate(r, timestamp, 60)
    
    # 4. Stocker les résultats dans Redis (Hashes)
    r.hset('crypto:agg:1m', mapping={'vwap': agg_1m[0], 'volume': agg_1m[1]})
    r.hset('crypto:agg:5m', mapping={'vwap': agg_5m[0], 'volume': agg_5m[1]})
    r.hset('crypto:agg:15m', mapping={'vwap': agg_15m[0], 'volume': agg_15m[1]})
    r.hset('crypto:agg:1h', mapping={'vwap': agg_1h[0], 'volume': agg_1h[1]})
    
    print(f"[Consumer 2 - Agg] 1m VWAP: {agg_1m[0]:.2f} (Vol: {agg_1m[1]:.2f}) | 1h Vol: {agg_1h[1]:.2f}")
