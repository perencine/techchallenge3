import requests, time, boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError

api_key = "54f03b5db6154b6fb5fb7fbed66772c6"  # Adquirida no site da API WeatherBit
lat = "-23.68"
lon = "46.46"  # Parâmetros de localização arbitrários
url = f"https://api.weatherbit.io/v2.0/current?lat={lat}&lon={lon}&key={api_key}"
cols_interesse = ['clouds', 'datetime', 'dewpt', 'dhi', 'dni', 'ghi', 'precip', 'pres', 'rh', 'slp', 'solar_rad', 'temp', 'ts', 'wind_dir', 'wind_spd']
arquivo = 'weather_reducao_colunas.csv'
aws_access_key_id=''
aws_secret_access_key=''
aws_session_token=''

def handle_s3(file_name, bucket, action, path_s3_download=None, prefix=None):
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )
    s3_client = session.client('s3')

    try:
        if action == 'upload':
            object_name = file_name.split('/')[-1]
            if prefix:
                object_name = f"{prefix}/{object_name}"
            s3_client.upload_file(file_name, bucket, object_name)
        elif action == 'download':
            s3_client.download_file(bucket, path_s3_download, file_name)
        else:
            print(f"Ação '{action}' não reconhecida.")
            return False
    except NoCredentialsError:
        print("As credenciais não estão disponíveis")
        return False
    except ValueError as ve:
        print(ve)
        return False
    except Exception as e:
        print(f"Erro ao realizar ação '{action}' no arquivo: {e}")
        return False

    return True    

if handle_s3(arquivo, 'weather-data-techchallenge3', 'download', path_s3_download='weather_reducao_colunas.csv'):
    print('Download do arquivo do S3 bem-sucedido. Preenchendo com informações da API...')
    # três tentativas de contato com a API
    for tentativas in range(1, 4):
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            new_line = {}
            for key, vlr in data['data'][0].items():
                if key in cols_interesse:
                    if key == 'datetime': vlr = vlr[:10]
                    new_line[key] = vlr
            if len(cols_interesse) == len(new_line):  # se veio todos os campos da lista cols_interesse, poderemos inserir a nova linha
                df_weather = pd.read_csv(arquivo)
                nova_condicao = pd.DataFrame([new_line])
                df_weather = pd.concat([df_weather, nova_condicao], ignore_index=True)
                df_weather.to_csv(arquivo, index=False)
                print('Inserção de informações concluída com sucesso')
                if handle_s3(arquivo, 'weather-data-techchallenge3', 'upload'): print('Upload do arquivo completo ao S3 bem-sucedido')
                break
        else: print('Erro ao obter informações da API.')
        if tentativas == 2: print('Não tentarei novamente')
        else: print(f'Tentando novamente... {tentativas}/3')
        time.sleep(3)
