import json
import websocket
from confluent_kafka import Producer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'crypto.trades.raw'

# Initialisation du producteur Kafka
try:
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    print(f"✅ Connecté à Kafka sur {KAFKA_BROKER}")
except Exception as e:
    print(f"❌ Erreur de connexion à Kafka: {e}")
    exit(1)

def on_message(ws, message):
    data = json.loads(message)
    # Les données de trade Binance ont 'p' pour prix et 'q' pour quantité
    prix = data.get('p')
    quantite = data.get('q')
    
    # Envoi dans le topic Kafka
    producer.produce(KAFKA_TOPIC, value=json.dumps(data).encode('utf-8'))
    producer.poll(0)
    print(f"🚀 Envoyé: {quantite} BTC @ {prix} USDT dans '{KAFKA_TOPIC}'")

def on_error(ws, error):
    print(f"❌ Erreur WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("🔴 Connexion WebSocket fermée")

def on_open(ws):
    print("🟢 Connecté au WebSocket Binance (btcusdt@trade)")

if __name__ == "__main__":
    # URL fournie pour le stream
    ws_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # Lancement du WebSocket
    ws.run_forever()
