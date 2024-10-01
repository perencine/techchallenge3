import requests
import pandas as pd
from datetime import datetime, timedelta

# Parâmetros de API
api_key = "54f03b5db6154b6fb5fb7fbed66772c6"
lat = "-23.68"
lon = "46.46"
today = datetime.now()
five_years_ago = today - timedelta(days=5*365)
weather_data = []

for year in range(five_years_ago.year, today.year + 1):
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31" if year != today.year else today.strftime('%Y-%m-%d')
    
    print(f"Coletando dados de {start_date} a {end_date}...")
    url = f"https://api.weatherbit.io/v2.0/history/daily?start_date={start_date}&end_date={end_date}&lat={lat}&lon={lon}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        weather_data.extend(data['data'])
    else:
        print(f"Erro: {response.status_code}")
        
df = pd.DataFrame(weather_data)
df.to_csv("techchallenge/arquivos/weather_completo.csv", index=False)
