from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import language_v2
from collections import defaultdict
import logging
import pandas as pd
import numpy as np
from datetime import date,datetime, timedelta
from tqdm import tqdm
import re
import os
from tqdm import tqdm
import json
import random
import time
from functools import wraps

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
BIGQUERY_TABLE = 'expansion_analisis_temas'  # BigQuery Table name where the data will be stored

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


def create_nlp_client():
    """
    Crea y devuelve un cliente autenticado para Google Cloud Natural Language API
    usando credenciales JSON cargadas desde una variable de entorno.
    
    Requiere que la variable de entorno 'NLP_APPLICATION_CREDENTIALS' contenga
    el JSON como una cadena de texto.
    
    :return: Cliente autenticado de Natural Language API.
    """
    nlp_key_json = os.getenv("NLP_APPLICATION_CREDENTIALS")
    credentials = service_account.Credentials.from_service_account_info(json.loads(nlp_key_json))
    client = language_v2.LanguageServiceClient(credentials=credentials)
    return client

def rate_limited(batch_size=20, delay=2):
    """Decorador que aplica un delay cada batch_size llamadas."""
    def decorator(func):
        counter = {"count": 0}  # Usamos un diccionario para que sea mutable

        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            counter["count"] += 1
            if counter["count"] % batch_size == 0:
                time.sleep(delay)
            return result
        return wrapper
    return decorator

@rate_limited(batch_size=20, delay=2)
def classify_text(text):
    """ Clasifica un texto usando la API de Cloud Natural Language y devuelve la categoría con mayor confianza. """
    client = create_nlp_client()

    document = language_v2.Document(
        content=text,
        type_=language_v2.Document.Type.PLAIN_TEXT
    )

    try:
        response = client.classify_text(request={"document": document})
        if response.categories:
            # Obtener la categoría con mayor confianza
            top_category = max(response.categories, key=lambda x: x.confidence)
            time.sleep(0.5)
            return top_category.name
        else:
            return "Sin clasificación"
    except Exception as e:
        logger.error(f"Error al clasificar el texto: {text}\n{e}")
        return "Error"

def extract_categories(category_path):
    """
    Extrae el primer y segundo nivel de una categoría en formato '/A/B/C'.
    Si no existen niveles suficientes, asigna 'Sin categoría'.
    """
    if category_path == "Sin categoría" or category_path == "Error":
        return "Sin categoría", "Sin categoría"

    category_levels = category_path.split("/")  # Separar por "/"
    
    categoria_1 = category_levels[1] if len(category_levels) > 1 else "Sin categoría"
    categoria_2 = category_levels[2] if len(category_levels) > 2 else "Sin categoría"
    categoria_3 = category_levels[3] if len(category_levels) > 3 else "Sin categoría"
    categoria_4 = category_levels[4] if len(category_levels) > 4 else "Sin categoría"



    return categoria_1, categoria_2,categoria_3, categoria_4

def main():
    bigquery_client = create_bigquery_client()

    query = """
    SELECT a.*
    FROM `bigquery-418518.ga4_analisis_topics.archivo_notas_expansion` a
    LEFT JOIN (
      SELECT DISTINCT pagePathPlusQueryString
      FROM `bigquery-418518.ga4_analisis_topics.expansion_analisis_temas`
    ) t
    ON a.pagePathPlusQueryString = t.pagePathPlusQueryString
    WHERE t.pagePathPlusQueryString IS NULL
      AND a.fecha_pub >= TIMESTAMP('2024-06-01')
      AND LOWER(a.Titulo) != "página no encontrada (error 404)"
    """

    notas = bigquery_client.query(query).to_dataframe()
    logger.info(f"{len(notas)} paths no están en expansion_analisis_temas (ni parcialmente)")

    if notas.empty:
        logger.info("No hay notas nuevas por actualizar")
        return

    # Construir texto para clasificación
    notas["Label_temas"] = notas["Titulo"] + ". " + \
                           notas["Descripcion"] + ". " + \
                           notas["Contenido"]

    # Truncar a 600 caracteres
    notas['Label_temas'] = notas['Label_temas'].apply(lambda x: x[:600] if len(x) > 600 else x)

    # Log de caracteres totales
    total_temas = notas['Label_temas'].astype(str).str.len().sum()
    logger.info(f"Total de caracteres en Label_temas: {total_temas}")

    # Clasificación de texto
    tqdm.pandas()
    notas["Tema"] = notas["Label_temas"].progress_apply(classify_text)

    # Extracción de categorías
    notas[["Categoría 1", "Categoría 2", "Categoría 3", "Categoría 4"]] = notas["Tema"].apply(
        lambda x: pd.Series(extract_categories(x))
    )

    # Normalizar fecha
    notas['fecha_pub'] = notas['fecha_pub'].dt.tz_localize(None)

    # Subida a BigQuery
    upload_to_bigquery(notas, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)