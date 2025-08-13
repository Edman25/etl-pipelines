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
BIGQUERY_TABLE = 'expansion_analisis_entidades'  # BigQuery Table name where the data will be stored

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
def analyze_entities(text, page_path, encoding_type=language_v2.EncodingType.UTF8):
    """
    Analiza entidades con Google Cloud NLP v2 según reglas:
    - No duplica entidades
    - Cuenta número de menciones
    - Filtra entidades tipo OTHER, NUMBER y menciones tipo COMMON
    - Filtra por probabilidad >= 0.7
    - Siempre devuelve al menos una fila con pagePath, aunque no haya entidades válidas
    """
    client = create_nlp_client()

    document = {
        "content": text,
        "type_": language_v2.Document.Type.PLAIN_TEXT,
        "language_code": "es"
    }

    try:
        response = client.analyze_entities(request={"document": document, "encoding_type": encoding_type})
        entity_records = []

        for entity in response.entities:
            entity_type = language_v2.Entity.Type(entity.type_).name

            if entity_type in ["OTHER", "NUMBER"]:
                continue

            filtered_mentions = [
                mention for mention in entity.mentions
                if language_v2.EntityMention.Type(mention.type_).name != "COMMON"
                and hasattr(mention, "probability")
                and mention.probability >= 0.7
            ]

            if not filtered_mentions:
                continue

            entity_name = entity.name
            num_menciones = len(filtered_mentions)
            prob_mencion_max = max(mention.probability for mention in filtered_mentions)

            entity_records.append({
                "pagePath": page_path,
                "Entidad": entity_name,
                "Tipo": entity_type,
                "Menciones": num_menciones,
                "Probabilidad": round(prob_mencion_max, 2)
            })

        # Si no hubo entidades válidas, devolver al menos una fila con nulls
        if not entity_records:
            return pd.DataFrame([{
                "pagePath": page_path,
                "Entidad": None,
                "Tipo": None,
                "Menciones": None,
                "Probabilidad": None
            }])

        return pd.DataFrame(entity_records)

    except Exception as e:
        logger.error(f"Error al analizar texto: {text}\n{e}")

def main():
    bigquery_client = create_bigquery_client()

    query = """
    SELECT a.*
    FROM `bigquery-418518.ga4_analisis_topics.archivo_notas_expansion` a
    LEFT JOIN (
      SELECT DISTINCT pagePath
      FROM `bigquery-418518.ga4_analisis_topics.expansion_analisis_entidades`
    ) t
    ON a.pagePathPlusQueryString = t.pagePath
    WHERE t.pagePath IS NULL
      AND a.fecha_pub >= TIMESTAMP('2024-06-01')
      AND LOWER(a.Titulo) != "página no encontrada (error 404)"
    """

    notas = bigquery_client.query(query).to_dataframe()
    logger.info(f"{len(notas)} paths no están en expansion_analisis_entidades (ni parcialmente)")
    if notas.empty:
        logger.info("No hay notas nuevas por actualizar")
        return

    notas["Label_entidades"] = notas["Titulo"] + ". " + notas["Descripcion"]
    # Truncamos labels a 600
    notas["Label_entidades"] = notas["Label_entidades"].apply(lambda x: x[:600] if len(x) > 600 else x)

    total_entidades = notas['Label_entidades'].astype(str).str.len().sum()
    logger.info(f"Total de caracteres en Label_entidades: {total_entidades}")

    tqdm.pandas()
    resultados_entidades = []

    for _, row in tqdm(notas.iterrows(), total=len(notas)):
        df_entidades = analyze_entities(row["Label_entidades"], page_path=row["pagePathPlusQueryString"])
        if not df_entidades.empty:
            resultados_entidades.append(df_entidades)

    if not resultados_entidades:
        logger.info("No se encontraron entidades para subir.")
        return

    entidades_final = pd.concat(resultados_entidades, ignore_index=True)

    entidades_final = entidades_final.astype({
        "pagePath": "string",
        "Entidad": "string",
        "Tipo": "string",
        "Menciones": "Int64",
        "Probabilidad": "float64"
    })

    upload_to_bigquery(entidades_final, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)

       
