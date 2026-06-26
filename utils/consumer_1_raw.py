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
    print(f"✅ [Consumer 1] Connecté à Redis sur {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"❌ [Consumer 1] Erreur Redis: {e}")
    exit(1)

try:
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'consumer-group-1-raw',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"✅ [Consumer 1] Connecté à Kafka, topic: '{KAFKA_TOPIC}'")
except Exception as e:
    print(f"❌ [Consumer 1] Erreur Kafka: {e}")
    exit(1)

print("🔄 [Consumer 1] En attente de trades pour stockage brut...")

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
        timestamp = int(data.get('E')) # Event time en ms
    except (TypeError, ValueError) as e:
        continue
    
    trade_data = json.dumps({
        "p": price,
        "q": quantity,
        "t": timestamp
    })
    
    # Ajout dans un Sorted Set Redis, scoré par le timestamp
    r.zadd('crypto:trades:raw', {trade_data: timestamp})
    
    # Nettoyage optionnel (on garde les dernières 24h par exemple)
    cutoff = timestamp - 86400000
    r.zremrangebyscore('crypto:trades:raw', '-inf', cutoff)
    
    print(f"[Consumer 1 - Raw] Sauvegardé: {quantity:8.4f} BTC @ {price:.2f} USDT")
