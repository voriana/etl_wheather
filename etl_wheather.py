import os
import requests
import pandas as pd
from datetime import datetime
import json
from deltalake import write_deltalake, DeltaTable
from decouple import config
from pprint import pprint
from unidecode import unidecode

"""
 Se selecciona la API acuweather.com , que brinda varios endpoints
 sobre datos meteologicos por zonas -grupos -ciudades -busquedas especificas.
 Segun el endpoint consultado se puede obtener info actulizada por fecha-hora-
 o temp historicas

 Por Buenas practicas la API_KEY, se encuentra en un archivo .env
 Descripcion de ETL:
 "Consiste en obtener temperaturas diarias de las capitales de paises de Sur America,
  obteniendo las temp min, max y promedio por fecha para cada ciudad"
 
 Se consulta el endpoint de location_key para las capitales de paises
 de latinoAmerica. la location_key se usa para consultar los siguientes endpoints:
  - Endpoint de forecast (pronostico) de 1 dia de cada ciudad
  - Endpoint de search (busqueda) por ciudad para obtener datos de region y pais donde se ubiaca esa ciudad.
  - Para el endpoint forecast --> se aplica extraccion incremental
  - Para el endpoint search   --> se aplica extraccion full
 
 Lo datos obtenidos de cada endpoint se carga en un dataFrame (df) respectivamente.
 Los df's generados son almacenado como deltalake en directorio data en capa Bronze
 Se realizan transformaciones al df de pronosticos de ciudades y se realiza un merge con df de ciudad_detalles
 El df transformado se almacena en directorio dat en capa Silver

"""

# Configuración
base_url = 'http://dataservice.accuweather.com/'
api_key = config('accu_api_key') #correccion tp_01, no mostrar contraseñas  en el codigo
cities = ['Buenos Aires','Brasilia','Santiago','Bogotá', 'Quito', 'Georgetown', 'Asuncion', 'Lima', 'Paramaribo', 'Montevideo', 'Caracas']
# Construir la ruta absoluta al archivo
current_path = os.path.dirname(os.path.abspath(__file__))  # Ubicación del script actual
last_dates_file = os.path.join(current_path, "metadata", "last_extracted_dates.json")  # Ruta completa

# Cargar fechas desde el archivo JSON
def load_last_extracted_dates(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

# Guardar fechas en el archivo JSON
def save_last_extracted_dates(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

# Obtener LocationKey -endpoint : locations/v1/cities/search 
def get_location_key(city_name, api_key):
    endpoint_url = f"{base_url}locations/v1/cities/search"
    params = {'apikey': api_key, 'q': city_name}
    try:
        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()
        data = response.json()
        if data:
            return data[0]['Key']
        else:
            print(f"No se encontraron resultados para {city_name}.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during request for {city_name}: {e}")
        return None
    
# Extraer pronóstico incremental -endpoint: forecasts/v1/daily/1day/{location_key}
# endpoint con campo tipo date
def extract_forecast(location_key, api_key, city_name, last_dates):
    """
    Extraer datos por dia, para una ciudad especifica. Actualizacion incremental por fecha
    parametros:
    location_key: Identificador único para la ciudad, obtenido de la función get_location_key.
                  Es esencial para consultar el pronóstico de la ciudad deseada.
    api_key: parametro que recibe la ApiKey que entrega la API para consulta de endpoints
    city_name: Parametro que recibe para el nombre de la ciudad, para obtener la fecha de ultima
    actualizacion de los datos
    last_dates: Diccionario que guarda las fechas de la última extracción para cada ciudad.
    """
    endpoint = f"forecasts/v1/daily/1day/{location_key}"
    endpoint_url = f"{base_url}{endpoint}"
    params = {'apikey': api_key}
    
    last_date = datetime.strptime(last_dates.get(city_name, "1900-01-01"), "%Y-%m-%d").date()
    
    try:
        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()
        forecast_data = response.json()
        
        forecast_date = datetime.strptime(forecast_data['DailyForecasts'][0]['Date'], "%Y-%m-%dT%H:%M:%S%z").date()
        
        if forecast_date > last_date:
            print(f"Nuevo pronóstico extraído para {city_name}: {forecast_date}")
            last_dates[city_name] = forecast_date.strftime("%Y-%m-%d")
            #return forecast_data
            return {
                'city': city_name,
                'date': forecast_date,
                'min_temp': forecast_data['DailyForecasts'][0]['Temperature']['Minimum']['Value'],
                'max_temp': forecast_data['DailyForecasts'][0]['Temperature']['Maximum']['Value'],
                'unit': forecast_data['DailyForecasts'][0]['Temperature']['Minimum']['Unit'],
                'day_icon': forecast_data['DailyForecasts'][0]['Day']['Icon'],
                'day_phrase': forecast_data['DailyForecasts'][0]['Day']['IconPhrase'],
                'night_icon': forecast_data['DailyForecasts'][0]['Night']['Icon'],
                'night_phrase': forecast_data['DailyForecasts'][0]['Night']['IconPhrase']
            }
        else:
            print(f"No hay nuevos pronósticos para {city_name}.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error durante la solicitud para {city_name}: {e}")
        return None

# Extraccion ciudad_detalles- endpoint: locations/v1/{location_key} 
def get_city_details(location_key, api_key):
    """
    Extrae detalles de la ciudad (incluyendo el país) usando el location_key.
    Extraccion Full
    
    Parámetros:
    location_key: Identificador de la ciudad.
    api_key: Clave de la API.
    
    Retorna:
    Diccionario con detalles de la ciudad, incluyendo el país  y region
    """
    endpoint = f"locations/v1/{location_key}"
    endpoint_url = f"{base_url}{endpoint}"
    params = {'apikey': api_key}

    try:
        response = requests.get(endpoint_url, params=params)
        response.raise_for_status()
        city_data = response.json()
        return city_data
    except requests.exceptions.RequestException as e:
        print(f"Error durante la solicitud de detalles de la ciudad: {e}")
        return None

# Crear DataFrame
def build_table(data_json):
    """
    Construye un DataFrame de pandas a partir de un objeto JSON.

    Parámetros:
    - data_json: objeto JSON a convertir en un DataFrame
    
    Retorna:
    - DataFrame
    """
    try:
        df_clima = pd.json_normalize(data_json)
        return df_clima
    except Exception as e:
        print(f"Error al intentar cargar el DataFrame: {e}")

# Formato Delta Lake
def formato_deltalake(carp_archivo_delta, data_frame):
    """
    Crea un archivo en formato Delta Lake en una carpeta específica.
    Agrega nuevos registros al archivo existente sin alterar los datos previos mode='append'

    Parámetros:
    - carp_archivo_delta: Nombre de la carpeta donde se creará el archivo Delta Lake
    - data_frame: DataFrame a guardar en formato Delta Lake
    """
    try:
        current_path = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_path, "data")
        os.makedirs(data_dir, exist_ok=True)

        write_deltalake(f"{data_dir}/{carp_archivo_delta}", data_frame, mode="append")
        print(f"Archivo Delta Lake creado en {data_dir}/{carp_archivo_delta}")
    except Exception as e:
        print(f"Error al crear el archivo Delta Lake: {e}")

#transfomar datos para persistir en capa silver
def transform_data():
    """
    Aplica transformaciones al dataframe de pronostico,toma el archivo parquet
    que se encuentre en la zona bronze

    Hace join de dataframe pronostico con dataframe ciudad_Detalles que se encuentran en la zona bronze
    Return: Df de pronostico con transformaciones + datos de dataframe ciudad_detalles
            (detalles de region y pais donde se encuentra la ciudad )
    """

    try:
        current_path = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_path, "data")
        dat_bronze = os.path.join(data_dir, "bronze\\forecasts_south.parquet")
        df =DeltaTable(dat_bronze).to_pandas()
             
        # #transformacion 1 :creacion columna nueva (temperatura en grados Celcius)
        df['min_temp_celcius'] = (df['min_temp']-32)*5/9
        df['max_temp_celcius'] = (df['max_temp']-32)*5/9
        
        
        # #transformacion 2 : convertir campos a float con precision 1
        df['min_temp_celcius'] = df['min_temp_celcius'].round(1)
        df['max_temp_celcius'] = df['max_temp_celcius'].round(1)

        # #transformacion 3 : promedio de temperaturas en min-max en celcius
        df['mean_temp_min_celcius'] = df['min_temp_celcius'].mean().round(1)
        df['mean_temp_max_celcius'] = df['max_temp_celcius'].mean().round(1)

        #transformacion 4 : Joinear el df de la extraccion full contra el df de pronostico de cada ciudad
        dat_city_bronze = os.path.join(data_dir, "bronze\\ciudad_detalles.parquet")
        df_cities =DeltaTable(dat_city_bronze).to_pandas()

        #merge 
        if not df.empty and not df_cities.empty:
            df_combined = pd.merge(df,df_cities,left_on='city', right_on='city', how='left')
        
        #transformacion 5:Filtrar solo columnas necesarias del df combined
        df_combined = df_combined[['city','date','min_temp','min_temp_celcius','mean_temp_min_celcius',
                                   'max_temp','max_temp_celcius','mean_temp_max_celcius', 'Country.ID',
                                   'Country.EnglishName','GeoPosition.Latitude','GeoPosition.Longitude',
                                   'TimeZone.GmtOffset']]
        
        # transformacion 6: Renombrar columnas
        df_combined = df_combined.rename(columns={
            'city': 'City', 
             'min_temp': 'min_temp(F)', 
             'min_temp_celcius': 'min_temp(C)',
             'mean_temp_min_celcius':'avg_temp(C)',
             'max_temp': 'max_temp(F)', 
             'max_temp_celcius': 'max_temp(C)',
             'mean_temp_max_celcius':'avg_temp_(C)',
             'Country.ID': 'pais_id',
             'Country.EnglishName': 'pais_nombre',
             'GeoPosition.Longitude': 'geo_lon',
             'GeoPosition.Latitude': 'geo_lat',
             'TimeZone.GmtOffset': 'GMT/UTC'
        })
        return df_combined 
    except Exception as e:
        print (f'Error durante transformaciones en el dataframe: {e}')
        return None

def main():
    os.makedirs("metadata", exist_ok=True)
    last_dates = load_last_extracted_dates(last_dates_file)
    forecast_list = []
    country_info = []

    for city in cities:
        location_key = get_location_key(city, api_key)
        if location_key:
            print(f"Location key found for {city}: {location_key}") 
            #extraccion incremental
            forecast_data = extract_forecast(location_key, api_key, city, last_dates)
            if forecast_data:
                forecast_list.append(forecast_data)
            # extraccion full
            city_details = get_city_details(location_key, api_key=api_key)
            if city_details:
                country_info.append({'city':city , 
                                     'country':city_details['Country']['EnglishName'],
                                     'details:':city_details})
                
       
    #Actualizar las fechas extraídas para extraccion incrementar
    save_last_extracted_dates(last_dates_file, last_dates)
     
    #Crear y guardar el DataFrame forecast_list extraccion incremental
    if forecast_list:
        df_forecast = pd.DataFrame()
        df_forecast = build_table(forecast_list)
# 
    # # # #Guardar en formato Delta Lake 
        if df_forecast is not None:
            formato_deltalake("bronze/forecasts_south.parquet", df_forecast)
    
    #guardar como Json ciudad detalles
    path_file = os.path.join(current_path,'metadata','ciudades_detalles.json')
    try:
        with open(path_file, mode='w', encoding='utf-8') as file:
            json.dump(country_info, file, ensure_ascii=False, indent=4)
            print(f"Archivo JSON guardado en {path_file}")
    except Exception as e:
        print(f"Error al guardar el archivo JSON: {e}")
   
    # # # dataframe de extraccion full    
    # (si lo guardo directamente en un dataframe seria asi)
    if country_info:
        df_cities = pd.DataFrame()
        df_cities = build_table(country_info)
  
    # # # dataframe de extraccion full (lectura desde un archivo json)
    path_file = os.path.join(current_path,'metadata','ciudades_detalles.json')
    print(path_file)
    with open(path_file, mode='r', encoding='utf-8') as file:
        data = json.load(file)

    df_cities = pd.json_normalize(data)
    # # Renombrar columnas en df_cities ( el formato deltalake da error si existen valores null o lista vacias [])
    df_cities = df_cities.rename(columns=lambda x: x.replace('details:.', ''))

# Verifica los nuevos nombres
    print(df_cities.columns)
    if df_cities is not None:
            df_cities.fillna('N/A',inplace=True)
            df_cities.convert_dtypes()
            #Convertir listas a cadenas (evitar error al crear archivo Deltalake por null o [])
            if 'SupplementalAdminAreas' in df_cities.columns:
                df_cities['SupplementalAdminAreas'] = df_cities['SupplementalAdminAreas'].astype(str)
            if 'DataSets' in df_cities.columns:
                df_cities['DataSets'] = df_cities['DataSets'].astype(str)
        
            formato_deltalake("bronze/ciudad_detalles.parquet", df_cities)
    
    #transformar valores del df forecasts y unir con df ciudad detales
    df_transform = transform_data()
    
    if df_transform is not None:
        formato_deltalake("silver/forecasts_join.parquet",df_transform)


# Ejecutar script
if __name__ == "__main__":
    main()
