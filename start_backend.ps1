# start_backend.ps1

Write-Host "Démarrage de l'infrastructure Docker (Kafka, Zookeeper, Redis)..." -ForegroundColor Cyan
docker-compose up -d

Write-Host "Attente de 5 secondes pour l'initialisation des conteneurs..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "Lancement des services Python dans des fenêtres séparées..." -ForegroundColor Cyan

# Chemin de l'environnement virtuel et commande d'activation
$activateCmd = ".\.venv\Scripts\activate"

# 1. Producer
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\producer.py" -WindowStyle Normal

# 2. Consumer 1 (Raw)
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\consumer_1_raw.py" -WindowStyle Normal

# 3. Consumer 2 (Aggregations)
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\consumer_2_agg.py" -WindowStyle Normal

# 4. Consumer 3 (Alerts)
Start-Process powershell -ArgumentList "-NoExit", "-Command", "$activateCmd; python utils\consumer_3_alerts.py" -WindowStyle Normal

Write-Host "✅ Tout le backend a été lancé avec succès !" -ForegroundColor Green
Write-Host "Vous devriez voir 4 nouvelles fenêtres de terminal apparaître."
