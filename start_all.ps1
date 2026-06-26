# start_all.ps1

Write-Host "Démarrage de l'infrastructure Docker (Kafka, Zookeeper, Redis)..." -ForegroundColor Cyan
docker-compose up -d

Write-Host "Attente de 5 secondes pour l'initialisation des conteneurs..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "Lancement des services Python, de l'API et du Dashboard..." -ForegroundColor Cyan

$activateCmd = ".\.venv\Scripts\activate"

# 1. Producer
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\producer.py" -WindowStyle Normal

# 2. Consumers
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\consumer_1_raw.py" -WindowStyle Normal
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\consumer_2_agg.py" -WindowStyle Normal
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\consumer_3_alerts.py" -WindowStyle Normal

# 3. API FastAPI
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python -m uvicorn api.main:app --reload --port 8000" -WindowStyle Normal

# 4. Dashboard (React + Vite)
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd dashboard; npm run dev" -WindowStyle Normal

Write-Host "✅ Tout a été lancé avec succès (6 fenêtres ouvertes) !" -ForegroundColor Green
Write-Host "Le Dashboard React sera bientôt disponible sur http://localhost:5173"
