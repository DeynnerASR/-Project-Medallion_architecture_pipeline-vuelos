import requests
import json
from datetime import datetime
from pathlib import Path


#URL = "https://openskynetwork.github.io/opensky-api/rest.html"
URL = "https://opensky-network.org/api/states/all"

def correr_ingestion_bronze(**context):
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    # Creare, guardare y referenciare el volumen de datos. Habrá un archivo que contenga la informacion por cada momento que se ejecute
    path = Path(f"/opt/airflow/data/bronze/vuelos_{timestamp}.json")

    with open(path,"w") as f:
        json.dump(data,f)

    # Comparto informacion a la siguiente tarea (task) dentro del dag, usando xcom 
    # El key es el identificador unico, y su valor en este caso sera la direccion en string
    context["ti"].xcom_push(key="bronze_file",value=str(path))
