import { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Activity, AlertTriangle, Zap, Server, BarChart2 } from 'lucide-react';
import './App.css';

function App() {
  const [data, setData] = useState({
    trades: [],
    stats: {
      "1m": { vwap: 0, volume: 0 },
      "5m": { vwap: 0, volume: 0 },
      "15m": { vwap: 0, volume: 0 },
      "1h": { vwap: 0, volume: 0 },
    },
    alerts: []
  });
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    let ws = null;
    
    const connectWs = () => {
      ws = new WebSocket('ws://localhost:8000/ws');
      
      ws.onopen = () => setIsConnected(true);
      ws.onclose = () => {
        setIsConnected(false);
        setTimeout(connectWs, 3000); // Reconnect logic
      };
      
      ws.onmessage = (event) => {
        const receivedData = JSON.parse(event.data);
        setData(receivedData);
      };
    };

    connectWs();
    
    return () => {
      if (ws) ws.close();
    };
  }, []);

  // Prepare chart data (Reversing trades to go from oldest to newest for left-to-right chart)
  const chartData = [...data.trades].reverse().map(t => ({
    time: new Date(t.t).toLocaleTimeString([], { hour12: false }),
    price: t.p
  }));

  const currentPrice = data.trades.length > 0 ? data.trades[0].p : 0;
  const price1hAgo = data.stats["1h"]?.vwap || currentPrice;
  const priceChange = price1hAgo > 0 ? ((currentPrice - price1hAgo) / price1hAgo) * 100 : 0;
  const isPositive = priceChange >= 0;

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1><Activity color="var(--accent-blue)" /> Crypto Market Monitor</h1>
        <div className="live-badge">
          <div className={`live-dot ${!isConnected ? 'offline' : ''}`}></div>
          {isConnected ? 'LIVE (Binance BTC/USDT)' : 'OFFLINE'}
        </div>
      </header>

      <section className="kpi-grid">
        <div className="glass-panel kpi-card">
          <h3>BTC/USDT — Prix actuel</h3>
          <p className="value">${currentPrice.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</p>
          <p className={`sub-value ${isPositive ? 'text-green' : 'text-red'}`}>
            {isPositive ? '▲' : '▼'} {Math.abs(priceChange).toFixed(2)}% vs VWAP 1h
          </p>
        </div>
        
        <div className="glass-panel kpi-card">
          <h3>Volume glissant 5 min</h3>
          <p className="value">{data.stats["5m"]?.volume?.toFixed(2) || 0} BTC</p>
          <p className="sub-value text-muted">VWAP: ${data.stats["5m"]?.vwap?.toFixed(2) || 0}</p>
        </div>

        <div className="glass-panel kpi-card">
          <h3>Volume glissant 1h</h3>
          <p className="value">{data.stats["1h"]?.volume?.toFixed(2) || 0} BTC</p>
          <p className="sub-value text-muted">VWAP: ${data.stats["1h"]?.vwap?.toFixed(2) || 0}</p>
        </div>

        <div className="glass-panel kpi-card">
          <h3>Anomalies récentes</h3>
          <p className={`value ${data.alerts.length > 0 ? 'text-yellow' : 'text-green'}`}>{data.alerts.length}</p>
          <p className="sub-value text-muted"><AlertTriangle size={14} /> Détectées en temps réel</p>
        </div>
      </section>

      <section className="main-grid">
        {/* Left Column: Chart & Alerts */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          <div className="glass-panel chart-panel" style={{ height: '400px' }}>
            <h3><BarChart2 size={16} /> Évolution du prix (Derniers trades)</h3>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" vertical={false} />
                <XAxis dataKey="time" stroke="var(--text-muted)" fontSize={12} tickMargin={10} />
                <YAxis domain={['auto', 'auto']} stroke="var(--text-muted)" fontSize={12} tickFormatter={(val) => `$${val}`} />
                <Tooltip 
                  contentStyle={{ backgroundColor: 'var(--card-bg)', borderColor: 'var(--border-color)', backdropFilter: 'blur(10px)' }}
                  itemStyle={{ color: 'var(--accent-blue)' }}
                />
                <Line type="monotone" dataKey="price" stroke="var(--accent-blue)" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>

          <div className="glass-panel alerts-panel">
            <h3><Zap size={16} /> Alertes Temps Réel</h3>
            <div className="alerts-list">
              {data.alerts.length === 0 ? (
                <p style={{ color: 'var(--text-muted)' }}>Aucune anomalie détectée pour l'instant.</p>
              ) : (
                data.alerts.map((alert, idx) => {
                  const time = new Date(alert.timestamp).toLocaleTimeString();
                  const isVol = alert.reasons[0].includes("Volume");
                  return (
                    <div className={`alert-item ${isVol ? 'critical' : ''}`} key={idx}>
                      <div className="alert-icon">
                        <AlertTriangle size={18} />
                      </div>
                      <div className="alert-content">
                        <p>{isVol ? "Pic de volume anormal" : "Variation de prix anormale"}</p>
                        <span>{time} — {alert.reasons[0]}</span>
                      </div>
                    </div>
                  )
                })
              )}
            </div>
          </div>
        </div>

        {/* Right Column: Trades Feed & System Health */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          <div className="glass-panel trades-panel">
            <h3><Activity size={16} /> Trades Récents (Live Feed)</h3>
            <div className="trades-list">
              {data.trades.slice(0, 15).map((trade, idx) => (
                <div className="trade-item" key={trade.t + '-' + idx}>
                  <div>
                    <div className="trade-price" style={{ color: trade.p >= price1hAgo ? 'var(--accent-green)' : 'var(--accent-red)' }}>
                      ${trade.p.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                    </div>
                    <div className="trade-time">Binance • {new Date(trade.t).toLocaleTimeString()}</div>
                  </div>
                  <div className="trade-qty">
                    {trade.q.toFixed(4)} BTC
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="glass-panel health-panel">
            <h3><Server size={16} /> Pipeline — Santé Système</h3>
            <div className="health-item">
              <span>WebSocket Binance</span>
              <span className="status"><div className="live-dot" style={{width: 6, height: 6}}></div> connecté</span>
            </div>
            <div className="health-item">
              <span>API Backend</span>
              <span className={`status ${!isConnected ? 'warning' : ''}`}>{isConnected ? 'connecté' : 'déconnecté'}</span>
            </div>
            <div className="health-item">
              <span>Base de données</span>
              <span className="status">Redis</span>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default App;
