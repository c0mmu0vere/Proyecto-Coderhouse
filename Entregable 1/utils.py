import requests
import psycopg2
from variables import *

def extract_weather_data(provincias, api_key):
    response_objects = []  # Usamos una lista para almacenar las respuestas individuales

    for provincia in provincias:
        url_clima = f'http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={provincia}&days=1&aqi=no&alerts=no'
        response = requests.get(url_clima).json()
        response_objects.append(response)

    return response_objects

def transform_weather_data(response_objects, provincias):
    transformed_data = {}  # Usamos un diccionario para almacenar los datos transformados por provincia
    
    for provincia, response in zip(provincias, response_objects):
        current_data = response['current']  # Extraemos solo los datos 'current' del objeto de respuesta
        
        # Eliminamos los campos que no deseamos
        fields_to_remove = ['gust_mph', 'wind_mph', 'wind_dir', 'precip_in', 'wind_degree', 'last_updated_epoch', 'vis_miles', 'vis_km']
        for field in fields_to_remove:
            if field in current_data:
                del current_data[field]
        
        # Modificamos el campo 'condition' para quedarnos solo con el campo 'text'
        if 'condition' in current_data and 'text' in current_data['condition']:
            current_data['condition'] = current_data['condition']['text']

        transformed_data[provincia] = current_data

    return transformed_data

def load_weather_data(data_transformed):
    try:
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        
        cursor = conn.cursor()
        
        for provincia, data in data_transformed.items():
            table_name = provincia.lower()  # Suponiendo que los nombres de tabla son igual a las provincias en minúsculas
            columns = ', '.join(data.keys())
            values = ', '.join(['%s' for _ in data.keys()])
            
            insert_query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({values});'
            insert_data = tuple(data.values())
            
            cursor.execute(insert_query, insert_data)
            conn.commit()
            
            print(f"Datos insertados en {table_name}")
    except psycopg2.Error as e:
        print(f"Error de base de datos: {e}")
    finally:
        if conn:
            conn.close()

def run_weather_ETL(provincias, api_key):
    weather_data = extract_weather_data(provincias, api_key)
    data_transformed = transform_weather_data(weather_data, provincias)
    load_weather_data(data_transformed)



