import requests
import pandas as pd
import time
from datetime import datetime

api_key = "54f03b5db6154b6fb5fb7fbed66772c6"
lat = "-23.68"
lon = "46.46"
url = f"https://api.weatherbit.io/v2.0/current?lat={lat}&lon={lon}&key={api_key}"
cols_interesse = ['clouds', 'datetime', 'dewpt', 'dhi', 'dni', 'ghi', 'precip', 'pres', 'rh', 'slp', 'solar_rad', 'temp', 'ts', 'wind_dir', 'wind_spd']
arquivo = 'tehcchallenge3/arquivos/weather - reducao colunas.csv'

# três tentativas de contato com a API
for tentativas in range(3):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        new_line = {}
        for key, vlr in data['data'][0].items():
            if key == 'datetime': vlr = datetime.strptime(vlr, "%Y-%m-%d:%H").strftime("%Y-%m-%d")
            if key in cols_interesse: new_line[key] = vlr
        if len(cols_interesse) == len(new_line):  # se veio todos os campos da lista cols_interesse, sucesso
            df_weather = pd.read_csv(arquivo)
            nova_condicao = pd.DataFrame([new_line])
            df_weather_completo = pd.concat([df_weather, nova_condicao], ignore_index=True)
            df_weather_completo.to_csv(arquivo, index=False)
            break
    time.sleep(3)
