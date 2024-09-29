#!/usr/bin/env python

import json
import requests
import time
import os
import logging
import signal
from datetime import datetime, timezone
from tqdm import tqdm

# Archivo para registrar la lista "owner/repo" de los repositorios ya procesados
PROCESSED_REPOS_FILE = 'xRESTrepos_leidos.txt'
# Archivo para registrar metadatos de repos filtrados
METADATA_FILE = 'xRESTmetadata.json'

# Token de GitHub
GITHUB_TOKEN = open("github_token").read().strip()

# Configurar el logger
logger = logging.getLogger()

# Variables globales para uso en el handler de señal
repos_leidos = set()  # Para almacenar la lista de repositorios leídos en memoria
metadata_file = None  # Para manejar el archivo de metadatos


def setup_logging():
    """
    Configura el logging para consola y archivo
    """
    global logger
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        logger.handlers.clear()

    # Configurar handler de consola solo para mostrar el número de repos procesados
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(message)s')  # Solo el número de repos leídos
    console_handler.setFormatter(console_formatter)

    # Configurar handler de archivo para mensajes detallados
    file_handler = logging.FileHandler('script.log')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


# Manejo de interrupción de teclado para guardar progreso
def signal_handler(sig, frame):
    logger.info("\nInterruption detected. Saving progress and exiting...")
    if metadata_file:
        metadata_file.flush()  # Asegurarse de que los datos se escriben en disco
        metadata_file.close()  # Cerrar el archivo de metadatos
    exit(0)

signal.signal(signal.SIGINT, signal_handler)


# Función para cargar los repos ya procesados desde el archivo
def cargar_repos_leidos():
    if os.path.exists(PROCESSED_REPOS_FILE):
        with open(PROCESSED_REPOS_FILE, 'r') as f:
            repos = {line.strip() for line in f.readlines()}
            return repos
    return set()


# Guardar los repositorios leídos en el archivo
def guardar_repos_leidos(repos_leidos):
    with open(PROCESSED_REPOS_FILE, 'a') as f:
        for repo in repos_leidos:
            f.write(repo + "\n")
        f.flush()  # Asegurar que se escribe en disco


def capturar_metadatos_repositorios(repos_leidos, metadata_file):
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    url = "https://api.github.com/repositories"
    repos_procesados = 0
    max_retries = 5  # Número máximo de reintentos
    retry_delay = 10  # Tiempo de espera fijo entre reintentos en segundos
    params = {
        'since': 0,  # Empieza desde el primer repo (puede ajustarse para orden alfabético)
        'per_page': 100  # Máximo de repositorios por página permitido por GitHub
    }

    while True:
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logger.debug(f"REST query status != 200. --> {response.status_code}")
                    raise Exception(f"REST query failed with status {response.status_code}: {response.text}")

                # Control de límite de tasa
                rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                rate_limit_reset = int(response.headers.get("X-RateLimit-Reset", 0))

                if rate_limit_remaining == 0:
                    reset_time = max(0, rate_limit_reset - time.time())
                    reset_datetime = datetime.fromtimestamp(rate_limit_reset, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    logger.info(f"Rate limit reached. Waiting for {reset_time} seconds until {reset_datetime}...")
                    time.sleep(reset_time)

                repositories = response.json()

                if not repositories:
                    logger.info("No more repositories to process.")
                    return

                # Inicializar tqdm con el total de repositorios a procesar en esta página
                with tqdm(total=len(repositories), desc="Processing repositories", unit="repo") as pbar:
                    repos_guardar = set()

                    for repo_data in repositories:
                        repo_name = f"{repo_data['owner']['login']}/{repo_data['name']}"

                        if repo_name in repos_leidos:
                            logger.debug(f"Saltando {repo_name}, ya procesado.")
                            continue

                        if repo_data['language'] is None:
                            logger.debug(f"Saltando {repo_name}, no tiene lenguaje principal.")
                            continue

                        # Guardar metadatos en el archivo de resultados
                        metadata_file.write(json.dumps(repo_data) + "\n")
                        metadata_file.flush()

                        repos_guardar.add(repo_name)
                        repos_procesados += 1
                        # Imprimir el número de repositorios procesados
                        print(f"\rRepositorios procesados: {repos_procesados}", end="")

                        # Actualizar barra de progreso
                        pbar.update(1)

                    # Guardar los repositorios leídos
                    guardar_repos_leidos(repos_guardar)
                    repos_leidos.update(repos_guardar)

                # Actualizar el parámetro "since" para continuar desde el último repo procesado
                if repositories:
                    params['since'] = repositories[-1]['id']

                break  # Salir del bucle de reintentos si la solicitud fue exitosa

            except requests.exceptions.RequestException as e:
                logger.error("Request failed: %s", e)
                retries += 1
                time.sleep(retry_delay)  # Espera fija
                if retries >= max_retries:
                    logger.error("Max retries reached. Exiting...")
                    break


# Función principal de ejecución
def main():
    global repos_leidos
    global metadata_file

    setup_logging()

    # Cargar repositorios ya procesados
    repos_leidos = cargar_repos_leidos()

    logger.info(f"Cargando {len(repos_leidos)} repositorios ya procesados.")

    # Abrir el archivo de metadatos en modo append
    metadata_file = open(METADATA_FILE, 'a')

    try:
        # Capturar metadatos de los repositorios
        capturar_metadatos_repositorios(repos_leidos, metadata_file)
    finally:
        # Cerrar ficheros
        metadata_file.close()

if __name__ == "__main__":
    main()
