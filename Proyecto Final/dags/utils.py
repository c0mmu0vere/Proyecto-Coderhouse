import requests
import pandas as pd
from sqlalchemy import create_engine
from variables import *

def extract_weather_data(provincias, api_key):
    data_frames = [] 

    for provincia in provincias:
        url_clima = f'http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={provincia}&days=1&aqi=no&alerts=no'
        response = requests.get(url_clima).json()

        response['province'] = provincia
        df = pd.DataFrame([response])
        data_frames.append(df)

    result_df = pd.concat(data_frames, ignore_index=True)

    return result_df

def transform_weather_data(data_frame):
    provinces = data_frame['province']

    data_frame = data_frame.drop(columns=['location', 'forecast'])
    data_frame = pd.json_normalize(data_frame['current'])

    columns_to_remove = ['gust_mph', 'wind_mph', 'wind_dir', 'precip_in', 'wind_degree', 'last_updated_epoch', 'vis_miles', 'vis_km', 'condition.icon', 'condition.code']
    data_frame = data_frame.drop(columns=columns_to_remove)

    if 'condition.text' in data_frame.columns:
        data_frame['condition'] = data_frame['condition.text']
        data_frame = data_frame.drop(columns=['condition.text'])

    data_frame['province'] = provinces

    return data_frame

def load_weather_data(data_frame):
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        data_frame.to_sql(name = f'weather', con = engine, schema = f'{user}', if_exists='append', index=False)
        print("Datos insertados en Redshift")
    except Exception as e:
        print(f"Error al cargar datos en Redshift: {e}")