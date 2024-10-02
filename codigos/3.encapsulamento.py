import pandas as pd
import numpy as np
from datetime import datetime
import joblib
import boto3


df_bruto = pd.read_csv('Fiap/weather.csv')

aws_access_key_id=''
aws_secret_access_key='QL6pq'
aws_session_token=''
region_name = ''

def features_hist(df, dia, core):
    df = df.sort_values('datetime')

    features = ['clouds', 'dewpt', 'dhi', 'dni', 'ghi', 'precip', 'pres', 'rh', 'slp', 'solar_rad', 'temp', 'wind_dir', 'wind_spd']
    
    for feature in features:
        df[f"media_{feature}{core}_{dia}d"] = df[feature].rolling(window=dia, min_periods=1).mean()
        df[f"max_{feature}{core}_{dia}d"] = df[feature].rolling(window=dia, min_periods=1).max()
        df[f"min_{feature}{core}_{dia}d"] = df[feature].rolling(window=dia, min_periods=1).min()
        df[f"mediana_{feature}{core}_{dia}d"] = df[feature].rolling(window=dia, min_periods=1).median()
        df[f"cv_{feature}{core}_{dia}d"] = df[feature].rolling(window=dia, min_periods=1).std() / df[feature].rolling(window=dia, min_periods=1).mean()

    columns_to_return = ['datetime'] + \
        [f"media_{feature}{core}_{dia}d" for feature in features] + \
        [f"max_{feature}{core}_{dia}d" for feature in features] + \
        [f"min_{feature}{core}_{dia}d" for feature in features] + \
        [f"mediana_{feature}{core}_{dia}d" for feature in features] + \
        [f"cv_{feature}{core}_{dia}d" for feature in features]
    
    return df[columns_to_return]

def preparar_dados_para_previsao(df_novo):
    """
    Função para preparar os dados da mesma forma que foi feito no treinamento, 
    sem criar a coluna 'temp_next_day' já que estamos fazendo previsão.
    """
    df_novo['datetime'] = pd.to_datetime(df_novo['datetime'])
        

    df_features_1d = features_hist(df_novo, 1, "")
    df_features_2d = features_hist(df_novo, 2, "")
    df_features_3d = features_hist(df_novo, 3, "")
    df_features_7d = features_hist(df_novo, 7, "")
    df_features_14d = features_hist(df_novo, 14, "")
    df_features_30d = features_hist(df_novo, 30, "")

    df_novo = df_novo.merge(df_features_1d, on = "datetime", how = "left")
    df_novo = df_novo.merge(df_features_2d, on = "datetime", how = "left")
    df_novo = df_novo.merge(df_features_3d, on = "datetime", how = "left")
    df_novo = df_novo.merge(df_features_7d, on = "datetime", how = "left")
    df_novo = df_novo.merge(df_features_14d, on = "datetime", how = "left")
    df_novo = df_novo.merge(df_features_30d, on = "datetime", how = "left")


    df_novo['day_of_year'] = df_novo['datetime'].dt.dayofyear
    df_novo['month'] = df_novo['datetime'].dt.month
    df_novo['day_of_week'] = df_novo['datetime'].dt.dayofweek
    
    return df_novo

def carregar_modelo(nome_arquivo='modelo_treinado.pkl'):
    """
    Função para carregar o modelo salvo.
    """
    return joblib.load(nome_arquivo)

def prever_proximo_dia(df_novo, nome_arquivo='modelo_treinado.pkl'):
    """
    Função para fazer previsões com novos dados.
    :param df_novo: DataFrame contendo os novos dados (deve ter a mesma estrutura que os dados de treino)
    :param nome_arquivo: Nome do arquivo .pkl do modelo treinado
    :return: Previsão de temperatura para o próximo dia
    """
    model = carregar_modelo(nome_arquivo)

    df_novo_preparado = preparar_dados_para_previsao(df_novo)

    colunas_para_remover = ['datetime', 'dt_ref1d', 'dt_ref2d', 'dt_ref3d', 
                            'dt_ref7d', 'dt_ref14d', 'dt_ref30d']
    
    X_novo = df_novo_preparado.drop(columns=colunas_para_remover, errors='ignore')

    predicao = model.predict(X_novo.iloc[[-1]])
    X_last_day = X_novo.iloc[[-1]]
    temp_next_day_pred = model.predict(X_last_day)
    next_day_data = X_last_day.copy()
    next_day_data['datetime'] = df_novo['datetime'].max() + pd.Timedelta(days=1)
    next_day_data['temp'] = temp_next_day_pred
    next_day_data['temp_next_day'] = np.nan 
    next_day_data = next_day_data[['clouds', 'datetime', 'dewpt', 'dhi', 'dni', 'ghi', 'precip', 'pres',
       'rh', 'slp', 'solar_rad', 'temp', 'ts', 'wind_dir', 'wind_spd']]
    
    return next_day_data


previsao = prever_proximo_dia(df_bruto, nome_arquivo='Fiap/random_forest_weather_model.pkl')

previsao = previsao.rename(columns={'temp': 'predict_temp'})

previsao.to_csv(r"C:\Users\vitor\OneDrive\Área de Trabalho\Vitor\Modelos\Fiap\predict_next_day.csv", index=False)


#csv_file_path = 'predict_next_day.csv'

#s3 = boto3.client('s3', 
#                  aws_access_key_id=aws_access_key_id,
#                  aws_secret_access_key=aws_secret_access_key,
#                  aws_session_token=aws_session_token,
#                  region_name=region_name)

#bucket_name = 'weather-data-techchallenge3'
#s3_file_path = 'predict/predict_next_day.csv'

# Fazer o upload do arquivo CSV para o S3
#s3.upload_file(csv_file_path, bucket_name, s3_file_path)

#print(f"Arquivo {csv_file_path} foi enviado para {s3_file_path} no bucket {bucket_name}.")

