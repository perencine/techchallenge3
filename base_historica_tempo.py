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

# abaixo, apenas perfumaria
rename_dict = {
    'datetime': 'Data',
    'ts': 'Timestamp',
    'revision_status': 'Status Revisao',
    'pres': 'Pressao Media (mb)',
    'slp': 'Pressao ao Nivel do Mar (mb)',
    'wind_spd': 'Velocidade Vento (m/s)',
    'wind_gust_spd': 'Rajada Vento (m/s)',
    'max_wind_spd': 'Velocidade Max Vento (m/s)',
    'wind_dir': 'Direcao Vento (graus)',
    'max_wind_dir': 'Direcao Max Vento (graus)',
    'max_wind_ts': 'Horario Max Vento',
    'temp': 'Temp Media (°C)',
    'max_temp': 'Temp Max (°C)',
    'min_temp': 'Temp Min (°C)',
    'max_temp_ts': 'Horario Temp Max',
    'min_temp_ts': 'Horario Temp Min',
    'rh': 'Umidade Relativa (%)',
    'dewpt': 'Ponto Orvalho (°C)',
    'clouds': 'Cobertura Nuvens (%)',
    'precip': 'Precipitacao (mm)',
    'precip_gpm': 'Precipitacao Estimada (mm)',
    'snow': 'Neve Acumulada (mm)',
    'snow_depth': 'Profundidade Neve (mm)',
    'solar_rad': 'Radiacao Solar Media (W/m²)',
    't_solar_rad': 'Radiacao Solar Total (W/m²)',
    'ghi': 'Irradiacao Solar Horizontal Media (W/m²)',
    't_ghi': 'Irradiacao Solar Horizontal Total (W/m²)',
    'max_ghi': 'Irradiacao Solar Horizontal Max (W/m²)',
    'dni': 'Irradiacao Solar Direta Media (W/m²)',
    't_dni': 'Irradiacao Solar Direta Total (W/m²)',
    'max_dni': 'Irradiacao Solar Direta Max (W/m²)',
    'dhi': 'Irradiacao Solar Difusa Media (W/m²)',
    't_dhi': 'Irradiacao Solar Difusa Total (W/m²)',
    'max_dhi': 'Irradiacao Solar Difusa Max (W/m²)',
    'max_uv': 'Indice UV Max'
}

df.rename(columns=rename_dict, inplace=True)
print(df.head())
df.to_csv("weather.csv", index=False)
