# script basado en la idea del script de p.pelmer https://github.com/pelmers/github-repository-metadata

# Extrae metadata de repositorios de github.

# Realiza una consulta GraphQL a la API de GitHub para obtener repositorios con ciertas características.
# Los criterios de filtrado se definen en la "query".
# --> Actualmente son: tienen al menos un lenguaje de programación, creados desde el 2018, son públicos
# Guarda los resultados en un archivo.
# Hace logging informacional y debug a fichero y consola

#!/usr/bin/env python

import json
import requests
import time
import os
import logging
import signal
from datetime import datetime, timezone
from tqdm import tqdm

# Consulta GraphQL combinada para repos filtrados y su metadata
query_combined = """
query($after: String) {
  search(query: "created:>=2018-01-01 is:public language:*", type: REPOSITORY, first: 20, after: $after) {
    edges {
      node {
        ... on Repository {
          owner {
            login
            url
          }
          name
          description
          stargazerCount
          forkCount
          createdAt
          updatedAt
          pushedAt
          diskUsage
          licenseInfo {
            name
          }
          primaryLanguage {
            name
          }
          languages(first: 10) {
            nodes {
              name
            }
          }
          isArchived
          isEmpty
          isFork
          isInOrganization
          isPrivate
          isTemplate
          hasIssuesEnabled
          hasWikiEnabled
          hasProjectsEnabled
          hasSponsorshipsEnabled
          mergeCommitAllowed
          viewerCanSubscribe
          issues {
            totalCount
          }
          forks {
            totalCount
          }
          assignableUsers {
            totalCount
          }
          deployments {
            totalCount
          }
          environments {
            totalCount
          }
          milestones {
            totalCount
          }
          releases {
            totalCount
          }
          pullRequests {
            totalCount
          }
          watchers {
            totalCount
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Archivo para registrar la lista "owner/repo" de los repositorios ya procesados
PROCESSED_REPOS_FILE = 'xrepos_leidos.txt'
# Archivo para registrar metadatos de repos filtrados
METADATA_FILE = 'xmetadata.json'

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
    after_cursor = None
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    url = "https://api.github.com/graphql"
    repos_procesados = 0
    max_retries = 5  # Número máximo de reintentos
    retry_delay = 10  # Tiempo de espera fijo entre reintentos en segundos

    while True:
        variables = {"after": after_cursor}
        retries = 0
        while retries < max_retries:
            try:
                response = requests.post(url, json={'query': query_combined, 'variables': variables}, headers=headers)
                result = response.json()

                if response.status_code != 200:
                    logger.debug(f"GraphQL query status != 200. --> {response.status_code}")
                    raise Exception(f"GraphQL query failed with status {response.status_code}: {result}")

                if "errors" in result:
                    error_message = result["errors"][0]["message"]
                    logger.error("GraphQL errors encountered: %s", error_message)
                    if "timeout" in error_message or "bug" in error_message:
                        logger.info("Retrying due to temporary issue...")
                        retries += 1
                        time.sleep(retry_delay)  # Espera fija
                        continue
                    else:
                        break

                # Control de límite de tasa
                rate_limit_remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                rate_limit_reset = int(response.headers.get("X-RateLimit-Reset", 0))

                if rate_limit_remaining == 0:
                    reset_time = max(0, rate_limit_reset - time.time())
                    reset_datetime = datetime.fromtimestamp(rate_limit_reset, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    logger.info(f"Rate limit reached. Waiting for {reset_time} seconds until {reset_datetime}...")
                    time.sleep(reset_time)

                repositories = result["data"]["search"]["edges"]
                page_info = result["data"]["search"]["pageInfo"]

                # Inicializar tqdm con el total de repositorios a procesar en esta página
                with tqdm(total=len(repositories), desc="Processing repositories", unit="repo") as pbar:
                    repos_guardar = set()

                    for repo in repositories:
                        repo_data = repo['node']
                        repo_name = f"{repo_data['owner']['login']}/{repo_data['name']}"

                        if repo_name in repos_leidos:
                            logger.debug(f"Saltando {repo_name}, ya procesado.")
                            continue

                        if repo_data['primaryLanguage'] is None:
                            logger.debug(f"Saltando {repo_name}, no tiene lenguaje principal.")
                            continue

                        # Guardar metadatos en el archivo de resultados
                        metadata_file.write(json.dumps(repo_data) + "\n")
                        metadata_file.flush()

                        repos_guardar.add(repo_name)
                        repos_procesados += 1
                        # Imprimir el número de repositorios procesados
                        # Actualizar el número de repositorios procesados en la misma línea
                        print(f"\rRepositorios procesados: {repos_procesados}", end="")

                        # Actualizar barra de progreso
                        pbar.update(1)

                    # Guardar los repositorios leídos
                    guardar_repos_leidos(repos_guardar)
                    repos_leidos.update(repos_guardar)

                # Avanzar a la siguiente página si existe
                if page_info and page_info["hasNextPage"]:
                    after_cursor = page_info["endCursor"]
                else:
                    break

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
