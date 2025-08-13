from googleapiclient.discovery import build
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import httplib2
from collections import defaultdict
import logging
import pandas as pd
import numpy as np
from datetime import date,datetime, timedelta
from tqdm import tqdm
import requests
import base64
from typing import Union
import re
import os
import json

# Define el alcance de la autentificación para hacer consultas a la API
SCOPES = 'https://www.googleapis.com/auth/analytics.readonly'
# Ubicación de credenciales de Analytics
KEY_FILE_DICT = json.loads(os.getenv('ANALYTICS_APPLICATION_CREDENTIALS'))
# Crea variable para guardar credenciales
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_dict(KEY_FILE_DICT,scopes = SCOPES)
# Armamos autorización
service = build('analyticsdata', 'v1beta', credentials=CREDENTIALS)
# id de la propiedad
GA4_ID = os.getenv('GA4_CONSOLIDADO_ID')

# Prepara logging
# Obtén el objeto logger de mayor nivel
logger = logging.getLogger()
for handler in logger.handlers: logger.removeHandler(handler)
logger.setLevel(logging.INFO)
# Imprime a la consola
console = logging.StreamHandler()
logger.addHandler(console)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

BIGQUERY_PROJECT = 'bigquery-418518'  # Your Google Cloud Project ID
BIGQUERY_DATASET = 'ga4_analisis_topics'  # BigQuery Dataset name where the data will be stored
BIGQUERY_TABLE = 'editorial_type_consolidado_expansion_historico'  # BigQuery Table name where the data will be stored

def get_json_from_private_github_repo(owner: str, repo_name: str, file_path: str) -> Union[dict, None]:
    """Obtiene y decodifica un archivo JSON desde un repositorio privado de GitHub."""
    
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        logger.error("Error: La variable de entorno GITHUB_TOKEN no está configurada.")
        return None

    api_url = f"https://api.github.com/repos/{owner}/{repo_name}/contents/{file_path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Lanza excepción para errores HTTP (4xx/5xx)

        content_base64 = response.json()['content']
        decoded_content = base64.b64decode(content_base64).decode('utf-8')
        
        return json.loads(decoded_content)

    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP {e.response.status_code} al contactar la API de GitHub.")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexión: {e}")
        return None
    except (KeyError, json.JSONDecodeError):
        logger.error("Error: No se pudo decodificar el contenido o la respuesta de la API es inválida.")
        return None

logger.info("Obteniendo el diccionario de autores desde GitHub...")
    
# 1. Obtenermos diccionario de autores
autores_llaves = get_json_from_private_github_repo(owner="dataexpansion",
                                                   repo_name="Data-Expansion",
                                                   file_path="data-storage/autores_llaves.json")

# Definir el mapeo de categorías a nuevos valores
category_mapping = {
    "méxico": "Política",
    "mexico": "Política",
    "economia":"Economía",
    "relojes":"L&S",
    "cdmx":"Política",
    "deportes":"L&S",
    "elecciones":"Política",
    "internacional":"Internacional",
    "mundo":"Internacional",
    "infraestructura":"Obras",
    "interiorismo":"Obras",
    "construccion":"Obras",
    "estados": "Política",
    "viajes-y-gourmet":"L&S",
    "sociedad":"Política",
    "vida-arte":"Tendencias",
    "mercados ":"Mercados",
    "mercados":"Mercados",
    "mercadotecnia":"Empresas",
    "empresas":"Empresas",
    "tecnologia":"Tecnología",
    "voces":"Opinión",
    "presidencia":"Política",
    "arquitectura":"Obras",
    "finanzas-personales":"Economía",
    "finazas-personales":"Economía",
    "carrera":"Empresas",
    "actualidad":"Mujeres",
    "opinion":"Opinión",
    "estilo":"L&S",
    "desarrollo-inmobiliario":"Obras",
    "noticias-cine-y-tv":"L&S",
    "bespoke ad":"Otros",
    "congreso":"Política",
    "tendencias":"Tendencias",
    "entretenimiento":"L&S",
    "manufactura":"Obras",
    "opinion":"Opinión",
    "Política y Otros Datos":"Podcasts",
    "musica":"L&S",
    "Health Café":"Podcasts",
    "emprendedores":"Empresas",
    "autos":"L&S",
    "municipios": "Política",
    "Expansión Inmobiliario":"Otros",
    "Expansión Daily":"Podcasts",
    "Entrevistas":"Otros",
    "mundo":"Internacional",
    "Las 500 Empresas":"Otros",
    "ciencia-y-salud":"Otros",
    "liderazgo":"Mujeres",
    "Unicorn Hunters":"Podcasts"
}

# Función para crear el cliente de BigQuery
def create_bigquery_client():
    bigquery_key_json = os.getenv("BIGQUERY_APPLICATION_CREDENTIALS")
    credentials = service_account.Credentials.from_service_account_info(json.loads(bigquery_key_json))
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client


def upload_to_bigquery(df, project_id, dataset_id, table_id):
    # Crear cliente de BigQuery
    bigquery_client = create_bigquery_client()
    
    # Referencias al dataset y a la tabla
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # Generar esquema basado en las columnas del DataFrame
    schema = []
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = 'BOOLEAN'
        elif pd.api.types.is_datetime64_dtype(dtype):
            bq_type = 'TIMESTAMP'
        else:
            bq_type = 'STRING'
        schema.append(bigquery.SchemaField(col, bq_type))
    
    # Verificar si la tabla existe
    try:
        table = bigquery_client.get_table(table_ref)
        # Comprobar si el esquema coincide
        existing_schema = {field.name: field.field_type for field in table.schema}
        new_schema = {field.name: field.field_type for field in schema}
        
        if existing_schema == new_schema:
            # Si el esquema coincide, añadir datos
            load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
            load_job.result()  # Esperar a que la carga termine
            logger.info(f"Data appended to {table_id}")
        else:
            raise ValueError("El esquema de la tabla existente no coincide con el esquema del DataFrame.")
    
    except NotFound:
        # Si la tabla no existe, crearla
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
        load_job.result()  # Esperar a que la carga termine
        logger.info(f"Created table {table_id} and uploaded data")

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"Data uploaded to {full_table_id}")

def format_response(response, dimensions,metrics):
    # Parse Request
    report_data = defaultdict(list)
    for report in response.get('reports', []):
        rows = report.get('rows', [])
    for row in rows:
        for i, key in enumerate(dimensions):
            report_data[key].append(row.get('dimensionValues', [])[i]['value'])  # Get dimensions
        for i, key in enumerate(metrics):
            report_data[key].append(row.get('metricValues', [])[i]['value'])  # Get metrics
            
    df = pd.DataFrame(report_data)
    
    return df

def tag_dates(df):
    
    # Eliminar las filas donde 'fecha_pub' es NaT
    df = df.dropna(subset=['fecha_pub'])
    # Paso 1: Calcular la diferencia de días entre "Fecha" y "fecha_pub"
    df['days_since_pub'] = (df['Fecha'] - df['fecha_pub']).dt.days
    
    # Asegurarse de que la columna 'days_since_pub' sea del tipo int
    df['days_since_pub'] = df['days_since_pub'].astype('int64')
    
    # Paso 2: Crear una nueva columna para etiquetar los datos de "days_since_pub"
    def label_days(days):
        if days <= 6:
            return "New and Recent"
        elif days >= 7:
            return "Evergreen"
        else:
            return "Unknown"
    
    df['days_label'] = df['days_since_pub'].apply(label_days)
    
    df = df.reset_index(drop=True)
    return df

def extract_section(df, column_name):
    # Definir una expresión regular para extraer la sección de la URL
    section_pattern = re.compile(r'/([^/]+)/')
    
    # Función para extraer la sección de una URL
    def find_section(url):
        match = section_pattern.search(url)
        if match:
            return match.group(1)
        else:
            return 'No aplica'
    
    # Aplicar la función a la columna especificada
    df['sección'] = df[column_name].apply(find_section)
    
    return df

# Función para normalizar la ruta de página
def normalize_url(url):
    # Dividir la URL en base y parámetros de consulta
    base, _, _ = url.partition('?')
    # Eliminar cualquier slash final
    base = base.rstrip('/')
    return base
# Define una función personalizada para seleccionar un valor no '(not set)' en autor_cms
def seleccionar_autor_valido(serie):
    return serie[serie != '(not set)'].iloc[0] if not serie[serie != '(not set)'].empty else '(not set)'

def map_mesa_optimized(df, autor_mapping):
    """
    Mapea la columna 'Autor' en función del diccionario 'autor_mapping' y aplica casos especiales.
    
    Parámetros:
    df (pd.DataFrame): DataFrame con las columnas 'Autor', 'Mapped Section' y 'fecha_pub'.
    autor_mapping (dict): Diccionario con la estructura de mapeo de autores a mesas.

    Retorna:
    pd.DataFrame: DataFrame con la nueva columna 'mesa' mapeada.
    """
    
    # Crear un diccionario inverso para mapear autores a sus mesas directamente
    map_dict = {alias: mesa for mesa, autores in autor_mapping.items() for real_name, alias_list in autores.items() for alias in alias_list}
    
    # Mapear los valores de "Autor" a "mesa"
    df["mesa"] = df["Autor"].map(map_dict)
    
    # Caso especial 1: Si el autor es "José Luis Sánchez" y "Mapped Section" no es "Economía", poner NaN
    if "Mapped Section" in df.columns:
        mask_jls = (df["Autor"] == "José Luis Sánchez") & (df["Mapped Section"] != "Economía")
        df.loc[mask_jls, "mesa"] = np.nan

    # Caso especial 2: Modificado según la nueva condición
    if "fecha_pub" in df.columns and "Mapped Section" in df.columns:
        df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors='coerce')  # Convertir fechas
        
        mask_selene_tecnologia = (df["Autor"] == "Selene Ramírez") & (df["fecha_pub"].dt.year >= 2024) & (df["Mapped Section"] == "Tecnología")
        mask_selene_otro = (df["Autor"] == "Selene Ramírez") & ~mask_selene_tecnologia  # Cubre el caso contrario

        df.loc[mask_selene_tecnologia, "mesa"] = "Tecnología"
        df.loc[mask_selene_otro, "mesa"] = "GA"

    return df

def procesar_datos_autores(df):
    # Crear diccionario de búsqueda de autores
    dict_busqueda = {}
    for categoria, autores in autores_llaves.items():
        for autor, aliases in autores.items():
            for alias in aliases:
                dict_busqueda[alias] = autor

    # Transformar la columna 'Autor' usando el diccionario
    df['customEvent:autor_cms'] = df['customEvent:autor_cms'].apply(lambda nombre: dict_busqueda.get(nombre, nombre))
    
    # Reemplazar valores '' y 'noAuthor' con '(not set)'
    df['customEvent:autor_cms'] = df['customEvent:autor_cms'].replace(["", "noAuthor"], "(not set)")
    
    # Filtrar filas donde 'customEvent:autor_cms' no es '(not set)'
    # Este es el paso que da lata
    df = df[df['customEvent:autor_cms'] != '(not set)']
    # Renombrar columnas
    df.rename(columns={'customEvent:autor_cms': 'Autor'}, inplace=True)
    
    
    return df

def extract_date(df, column_name):
    df['pagePathPlusQueryString'] = df['pagePathPlusQueryString'].apply(normalize_url)
    
    
    # Paso 3: Agrupar por 'fecha_pub' y sumar 'totalUsers', 'sessions', 'screenPageViews'
    if 'totalUsers' in df.columns and 'sessions' in df.columns and 'screenPageViews' in df.columns:
        df_grouped = df.groupby(['Fecha', 'pagePathPlusQueryString']).agg({
            'totalUsers': 'sum',
            'customEvent:autor_cms': seleccionar_autor_valido,
            'sessions': 'sum',
            'screenPageViews': 'sum'
        }).reset_index()
    else:
        df_grouped = df.groupby(['Fecha', 'pagePathPlusQueryString']).size().reset_index(name='counts')
    
        
    # Define a regex pattern to find dates in the format yyyy-mm-dd
    date_pattern = re.compile(r'\d{4}/\d{2}/\d{2}')

    # Function to extract date from string
    def find_date(text):
        match = date_pattern.search(text)
        if match:
            return match.group(0).replace('/', '-')
        else:
            return np.nan
        

    # Apply the function to the specified column
    df_grouped['fecha_pub'] = df_grouped[column_name].apply(find_date)
    
    # Quitar las filas donde la fecha extraída es NaN
    # Otro potencial paso problemático
    df_grouped = df_grouped.dropna(subset=['fecha_pub'])
    
    # Convert the new column to datetime
    df_grouped['fecha_pub'] = pd.to_datetime(df_grouped['fecha_pub'], errors='coerce')
    
    return df_grouped
def tag_dates(df):
    
    # Eliminar las filas donde 'fecha_pub' es NaT
    df = df.dropna(subset=['fecha_pub'])
    # Paso 1: Calcular la diferencia de días entre "Fecha" y "fecha_pub"
    df['days_since_pub'] = (df['Fecha'] - df['fecha_pub']).dt.days
    
    # Asegurarse de que la columna 'days_since_pub' sea del tipo int
    df['days_since_pub'] = df['days_since_pub'].astype('int64')
    
    # Paso 2: Crear una nueva columna para etiquetar los datos de "days_since_pub"
    def label_days(days):
        if days <= 6:
            return "New and Recent"
        elif days >= 7:
            return "Evergreen"
        else:
            return "Unknown"
    
    df['days_label'] = df['days_since_pub'].apply(label_days)
    
    df = df.reset_index(drop=True)
    return df
def extract_section(df, column_name):
    # Definir una expresión regular para extraer la sección de la URL
    section_pattern = re.compile(r'/([^/]+)/')
    
    # Función para extraer la sección de una URL
    def find_section(url):
        match = section_pattern.search(url)
        if match:
            return match.group(1)
        else:
            return 'No aplica'
    
    # Aplicar la función a la columna especificada
    df['sección'] = df[column_name].apply(find_section)
    
    return df

def upload_after_processing(func):
    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)  # Ejecuta la función original y obtiene el DataFrame

        # Añades aquí el bloque "extra" de subir a BigQuery
        if not df.empty:
            upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
            logger.info("✅ Datos subidos exitosamente a BigQuery")
        else:
            logger.info("⚠️ DataFrame vacío. No se sube nada a BigQuery.")

        return df  # Retorna el DataFrame en caso de que lo necesites
    return wrapper

def request_author_data(startDate, endDate, analytics_view, extra_dimensions=None, alternative_metric=None, limit=100000, 
                metric_condition=None, dimension_filter=None, match_type=None, filter_value=None, 
                dimension_filter2=None, match_type2=None, filter_value2=None):
    '''
    Esta función especifica las métricas y dimensiones a consultar a la API de Analytics
    INPUT startDate: Fecha de inicio de la consulta (string)
          endDate: Fecha de término de la consulta (string)
          analytics_view: Propiedad de analytics a procesar (string)
          extra_dimensions: Lista de dimensiones adicionales a solicitar (lista de strings)
          limit: Límite de resultados a solicitar (entero)
          metric_filter: Métrica para aplicar el filtro (string)
          metric_filter_value: Valor para el filtro de métrica (int)
          metric_condition: Métrica para la ordenación (string)
          dimension_filter: Primera dimensión para aplicar el filtro (string)
          match_type: Tipo de coincidencia para el primer filtro (string)
          filter_value: Valor para el primer filtro (string)
          dimension_filter2: Segunda dimensión para aplicar el filtro (string)
          match_type2: Tipo de coincidencia para el segundo filtro (string)
          filter_value2: Valor para el segundo filtro (string)
    '''
    if dimension_filter is None and extra_dimensions:
        dimension_filter = extra_dimensions[0] if len(extra_dimensions) > 0 else None


    # Dimensiones a solicitar a Analytics
    dimensions = ['date']
    if extra_dimensions:
        dimensions.extend(extra_dimensions)  # Añade las dimensiones adicionales si se proporcionan
        
    if alternative_metric is None:
        # Métricas que vamos a solicitar
        metrics = ['totalUsers', 'sessions', 'screenPageViews']
    else:
        metrics = [alternative_metric]
    
    if metric_condition:
        # Construir la solicitud básica
        order_bys = [{"dimension": {"dimensionName": "date", "orderType": "ALPHANUMERIC"}},
                     {"metric": {"metricName": metric_condition}}]
    else:
        order_bys = [{"dimension": {"dimensionName": "date", "orderType": "ALPHANUMERIC"}, "desc": False}]
    
    request = {
        "requests": [
            {
                "dateRanges": [
                    {
                        "startDate": startDate,
                        "endDate": endDate
                    }
                ],
                "dimensions": [{'name': name} for name in dimensions],
                "metrics": [{'name': name} for name in metrics],
                "orderBys": order_bys,
                "limit": limit
            }
        ]
    }

    # Añadir filtro de dimensión si se ha especificado
    expressions = []
    if dimension_filter and match_type and filter_value:
        expressions.append({
            "filter": {
                "fieldName": dimension_filter,
                "stringFilter": {
                    "matchType": match_type,
                    "value": filter_value
                }
            }
        })

    if dimension_filter2 and match_type2 and filter_value2:
        expressions.append({
            "filter": {
                "fieldName": dimension_filter2,
                "stringFilter": {
                    "matchType": match_type2,
                    "value": filter_value2
                }
            }
        })

    if expressions:
        request['requests'][0]['dimensionFilter'] = {
            "andGroup": {
                "expressions": expressions
            }
        }

    response = service.properties().batchRunReports(property='properties/' + analytics_view, body=request).execute()
    df_request = format_response(response, dimensions, metrics)
    df_request = df_request.rename(columns={'date': 'Fecha'})
    # Convierte a datetime columna de fecha 
    # Convertir 'users', 'sessions', y 'pageviews' a integer
    if alternative_metric is None:
        df_request['totalUsers'] = df_request['totalUsers'].astype(int)
        df_request['sessions'] = df_request['sessions'].astype(int)
        df_request['screenPageViews'] = df_request['screenPageViews'].astype(int)
    elif alternative_metric == 'averageSessionDuration':
        df_request[alternative_metric] = df_request[alternative_metric].astype(float).round(2)
    else:
        df_request[alternative_metric] = df_request[alternative_metric].astype(int)
        
    df_request['Fecha'] = pd.to_datetime(df_request['Fecha'], format="%Y-%m-%d")
    
    # Ordenar por 'Fecha' y luego por 'totalUsers' y seleccionar las primeras 100 entradas si metric_condition no es None
    if metric_condition:
        df_request = df_request.sort_values(by=['Fecha', 'totalUsers'], ascending=[True, False]).head(100)
    
    return df_request

def gather_author_data(startDate, endDate, analytics_view, extra_dimensions=None, alternative_metric=None, limit=100000, 
                       metric_condition=None, dimension_filter=None, match_type=None, filter_value=None, 
                       dimension_filter2=None, match_type2=None, filter_value2=None):
    """
    Recopila datos diarios entre dos fechas utilizando la función request_day.

    :param startDate: Fecha de inicio (datetime or str)
    :param endDate: Fecha de fin (datetime or str)
    :param analytics_view: ID de la vista de GA
    :return: DataFrame con resultados concatenados
    """
    # Asegurarse de que ambas fechas sean datetime
    startDate = pd.to_datetime(startDate)
    endDate = pd.to_datetime(endDate)

    df_list = []
    iter_date = startDate

    total_days = (endDate - iter_date).days + 1
    with tqdm(total=total_days) as pbar:
        while iter_date <= endDate:
            try:
                day_str = iter_date.strftime('%Y-%m-%d')
                df = request_author_data(
                    day_str, day_str, analytics_view,
                    extra_dimensions=extra_dimensions,
                    alternative_metric=alternative_metric,
                    limit=limit,
                    metric_condition=metric_condition,
                    dimension_filter=dimension_filter, match_type=match_type, filter_value=filter_value,
                    dimension_filter2=dimension_filter2, match_type2=match_type2, filter_value2=filter_value2
                )
                df['totalUsers'] = df['totalUsers'].astype(int)
                df['Fecha'] = pd.to_datetime(day_str)
                df_list.append(df)
            except KeyError:
                logging.info(f"No hay datos disponibles para la fecha {day_str}")
            finally:
                iter_date += timedelta(days=1)
                pbar.update(1)

    if not df_list:
        return pd.DataFrame()  # Evita error en concat si no hubo datos

    full_result = pd.concat(df_list, ignore_index=True)

    if alternative_metric is None:
        full_result.sort_values(by=['Fecha', 'totalUsers'], ascending=[True, False], inplace=True)
        ordered_columns = ["Fecha", "totalUsers", "sessions", "screenPageViews"]
    else:
        full_result.sort_values(by=['Fecha', alternative_metric], ascending=[True, False], inplace=True)
        ordered_columns = ["Fecha", alternative_metric]

    remaining_columns = [col for col in full_result.columns if col not in ordered_columns]
    full_result = full_result[ordered_columns + remaining_columns]

    return full_result

def main():
    try:
        bigquery_client = create_bigquery_client()

        # Verificar última fecha disponible
        query = """
        SELECT MAX(DATE(Fecha)) AS max_fecha
        FROM `bigquery-418518.ga4_analisis_topics.editorial_type_consolidado_expansion_historico`
        """
        result = bigquery_client.query(query).result()
        max_fecha = [row.max_fecha for row in result][0]

        yesterday = date.today() - timedelta(days=1)

        if max_fecha == yesterday:
            logger.info("✅ Datos actualizados. No hay nuevas fechas que procesar.")
            return  # Salir del flujo

        # Continuar con el flujo solo si hay datos nuevos
        start_of_times = max_fecha + timedelta(days=1)
        end_of_times = yesterday

        logging.info(f"Pidiendo datos a Google Analytics desde {start_of_times} hasta {end_of_times}")

        # Procesamiento
        full_result = gather_author_data(start_of_times, end_of_times, GA4_ID,
                                         extra_dimensions=["pagePathPlusQueryString", "customEvent:autor_cms"])
        full_result = extract_date(full_result, 'pagePathPlusQueryString')
        full_result = procesar_datos_autores(full_result)
        full_result = tag_dates(full_result)
        full_result = extract_section(full_result, 'pagePathPlusQueryString')
        full_result['Mapped Section'] = full_result['sección'].map(category_mapping).fillna(full_result['sección'])
        full_result = map_mesa_optimized(full_result, autores_llaves)

        upload_to_bigquery(full_result, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)

        logger.info(f"✅ Datos procesados de {start_of_times} a {end_of_times} y subidos a BigQuery.")

    except Exception as e:
        logger.exception("❌ Error inesperado:")
