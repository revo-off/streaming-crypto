import json
import asyncio
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

app = FastAPI(title="Crypto Real-Time API")

# Configure CORS for Vite frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En production, spécifier le domaine du dashboard
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def fetch_current_data(r):
    # Fetch 50 last trades (le ZREVRANGE donne les scores les plus hauts en premier, donc les plus récents)
    raw_trades = r.zrevrange('crypto:trades:raw', 0, 49)
    trades = [json.loads(t) for t in raw_trades]

    # Fetch aggregates
    stats = {
        "1m": r.hgetall('crypto:agg:1m'),
        "5m": r.hgetall('crypto:agg:5m'),
        "15m": r.hgetall('crypto:agg:15m'),
        "1h": r.hgetall('crypto:agg:1h'),
    }

    # Format numeric values in stats
    for window in stats:
        for key in stats[window]:
            try:
                stats[window][key] = float(stats[window][key])
            except ValueError:
                pass

    # Fetch 10 last alerts
    raw_alerts = r.lrange('crypto:alerts', 0, 9)
    alerts = [json.loads(a) for a in raw_alerts]

    return {
        "trades": trades,
        "stats": stats,
        "alerts": alerts
    }

@app.get("/api/data")
async def get_data():
    r = get_redis()
    return fetch_current_data(r)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    r = get_redis()
    try:
        while True:
            data = fetch_current_data(r)
            await websocket.send_json(data)
            await asyncio.sleep(1) # Broadcast update every second
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"WS Error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
