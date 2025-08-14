import requests
from bs4 import BeautifulSoup
import json
import time
import numpy as np
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- Configuración del Script ---
CONFIG = {
    # ➡️ Estos valores se inicializan como None y se asignarán a partir de las variables de entorno.
    "output_dir": None,
    "state_dir": None,
    "max_workers": 9,
    "pause_geocode": 0.0,
    "base_url": "https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/informacionAbastecimientoActionCA.do?idRed=",
    "zip_range": (1, 25500)
}

def setup_environment():
    """Configura las rutas y el logger basándose en las variables de entorno."""

    # ➡️ Leer las variables de entorno para las rutas de salida y estado.
    # El método os.environ.get() es seguro y devuelve None si la variable no existe.
    CONFIG["output_dir"] = os.environ.get("OUTPUT_DIR")
    CONFIG["state_dir"] = os.environ.get("STATE_DIR")

    # ➡️ Validación crítica: comprueba si las variables de entorno están definidas.
    # Si no lo están, el script no puede ejecutarse correctamente y lanza un error.
    if not CONFIG["output_dir"] or not CONFIG["state_dir"]:
        raise ValueError("Las variables de entorno 'OUTPUT_DIR' y 'STATE_DIR' deben estar configuradas para que el script funcione.")

    # Construye las rutas completas para todos los archivos a partir de los directorios base.
    CONFIG["output_file"] = os.path.join(CONFIG["output_dir"], "abastecimientos_test.geojson")
    CONFIG["error_file"] = os.path.join(CONFIG["output_dir"], "abastecimientos_errors.geojson")
    CONFIG["cache_file"] = os.path.join(CONFIG["state_dir"], "geocode_cache.json")
    CONFIG["progress_file"] = os.path.join(CONFIG["state_dir"], "progress.json")
    CONFIG["log_file_prefix"] = os.path.join(CONFIG["state_dir"], "geocoding_")

    # Asegura que ambos directorios (salida y estado) existan.
    # Esto previene errores de "No such file or directory" antes de que el script intente escribir.
    os.makedirs(CONFIG["output_dir"], exist_ok=True)
    os.makedirs(CONFIG["state_dir"], exist_ok=True)

    # Configuración del logger
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = f"{CONFIG['log_file_prefix']}{fecha_hora}_test.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

# Llama a esta función al inicio del script para configurar todo el entorno antes de que comience el procesamiento.
setup_environment()

# --- Funciones de utilidad ---
def clean_text(text):
    return " ".join(text.strip().split())

# --- Manejo de caché ---
if os.path.exists(CONFIG["cache_file"]):
    with open(CONFIG["cache_file"], 'r', encoding='utf-8') as f:
        geocode_cache = json.load(f)
    logging.info(f"Se ha cargado el caché de geocodificación con {len(geocode_cache)} entradas.")
else:
    geocode_cache = {}
    logging.info("No se encontró archivo de caché, comenzando con caché vacío.")

def geocode_with_retry(localidad, idred, retries=3, backoff_factor=0.5):
    """Intenta geocodificar una localidad con reintentos en caso de fallo de red."""
    cache_key = f"{idred}::{localidad}"
    if cache_key in geocode_cache:
        logging.debug(f"Coordenadas de '{localidad}' (idRed={idred}) recuperadas del caché.")
        return geocode_cache[cache_key]
    url = "https://nominatim.openstreetmap.org/search"
    params = {"format": "json", "q": f"{localidad}, España"}
    headers = {"User-Agent": "GeoJSON-Extractor/1.0"}
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                coords = [lon, lat]
                geocode_cache[cache_key] = coords
                return coords
            else:
                geocode_cache[cache_key] = [0.0, 0.0]
                logging.warning(f"Geocodificación fallida para '{localidad}' (idRed={idred}): Respuesta vacía.")
                return [0.0, 0.0]
        except (requests.exceptions.RequestException, IndexError, ValueError) as e:
            logging.warning(f"Intento {i+1}/{retries} fallido para '{localidad}' (idRed={idred}): {e}")
            time.sleep(backoff_factor * (2 ** i))
    logging.error(f"Geocodificación fallida para '{localidad}' (idRed={idred}) después de {retries} intentos. No se añade al caché.")
    return [0.0, 0.0]

# --- Sesión de requests ---
session = requests.Session()
session.headers.update({
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
})

def obtener_y_procesar_datos(idred):
    """Obtiene los datos de la web de SINAC y construye las features."""
    features_ok = []
    features_error = []
    url = f"{CONFIG['base_url']}{idred}"
    
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        if not soup.find("th", text="Denominación"):
            logging.debug(f"idRed={idred} no contiene datos.")
            return [], []

        denominacion = clean_text(soup.find("th", text="Denominación").find_next("td").text)
        gestor = clean_text(soup.find("th", text="Gestor").find_next("td").text)
        localidades_html = soup.find("th", text="Localidades abastecidas").find_next("td")
        localidades = [clean_text(loc) for loc in localidades_html.stripped_strings]
        calidad = clean_text(soup.find("th", text="Calidad del agua").find_next("td").text)
        comentario_tag = soup.find("th", text="Comentario Aut. Sanitaria")
        comentario = clean_text(comentario_tag.find_next("td").text.strip()) if comentario_tag else "-"

        for loc in localidades:
            coords = geocode_with_retry(loc, idred)
            time.sleep(CONFIG['pause_geocode'])
            
            properties = {
                "Gestor": gestor,
                "Localidad": loc,
                "Calidad del agua": calidad,
                "Comentario": comentario
            }
            feature = {"type": "Feature", "properties": properties, "geometry": {"type": "Point", "coordinates": coords}}
            
            if coords == [0.0, 0.0]:
                feature["properties"]["idRed"] = idred
                feature["properties"]["denominacion"] = denominacion
                features_error.append(feature)
            else:
                features_ok.append(feature)
        
        return features_ok, features_error

    except (requests.exceptions.RequestException, Exception) as e:
        logging.error(f"Error al procesar idRed={idred}: {e}")
        return [], []

# --- Lógica principal de ejecución ---
def main():
    if os.path.exists(CONFIG["progress_file"]):
        with open(CONFIG["progress_file"], 'r', encoding='utf-8') as f:
            procesados = set(json.load(f))
        logging.info(f"Se han cargado {len(procesados)} IDs procesados previamente.")
    else:
        procesados = set()
        logging.info("No se encontró archivo de progreso, comenzando desde cero.")

    features_ok = []
    features_error = []
    
    if os.path.exists(CONFIG["output_file"]):
        with open(CONFIG["output_file"], 'r', encoding='utf-8') as f:
            features_ok = json.load(f)["features"]
        logging.info(f"Se han cargado {len(features_ok)} features del archivo de salida existente.")

    if os.path.exists(CONFIG["error_file"]):
        with open(CONFIG["error_file"], 'r', encoding='utf-8') as f:
            features_error = json.load(f)["features"]
        logging.info(f"Se han cargado {len(features_error)} features del archivo de errores existente.")

    lista_zip = list(map(int, np.arange(CONFIG["zip_range"][0], CONFIG["zip_range"][1], 1)))
    ids_a_procesar = [idred for idred in lista_zip if idred not in procesados]
    total_ids = len(lista_zip)
    total_a_procesar = len(ids_a_procesar)

    if total_a_procesar > 0:
        logging.info(f"Comenzando a procesar {total_a_procesar} IDs restantes de un total de {total_ids}.")
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
            futuros = {executor.submit(obtener_y_procesar_datos, idred): idred for idred in ids_a_procesar}

            for futuro in as_completed(futuros):
                idred = futuros[futuro]
                try:
                    nuevas_features_ok, nuevas_features_error = futuro.result()
                    if nuevas_features_ok:
                        features_ok.extend(nuevas_features_ok)
                    if nuevas_features_error:
                        features_error.extend(nuevas_features_error)
                    
                    procesados.add(idred)
                    with open(CONFIG["progress_file"], 'w', encoding='utf-8') as f:
                        json.dump(list(procesados), f, ensure_ascii=False)
                    
                    avance = (len(procesados) / total_ids) * 100
                    logging.info(f"AVANCE: Se ha procesado el ID {idred}. Progreso total: {len(procesados)}/{total_ids} ({avance:.2f}%)")
                
                except Exception as e:
                    logging.error(f"Error procesando el resultado del futuro para idRed={idred}: {e}")

    geojson_ok = {"type": "FeatureCollection", "features": features_ok}
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(geojson_ok, f, ensure_ascii=False, indent=4)

    if features_error:
        geojson_error = {"type": "FeatureCollection", "features": features_error}
        with open(CONFIG["error_file"], "w", encoding="utf-8") as f:
            json.dump(geojson_error, f, ensure_ascii=False, indent=4)
        logging.warning(f"Se han guardado {len(features_error)} features con errores de geocodificación en '{CONFIG['error_file']}'.")

    with open(CONFIG["cache_file"], 'w', encoding='utf-8') as f:
        json.dump(geocode_cache, f, ensure_ascii=False, indent=4)

    logging.info(f"Proceso completado. Total features OK: {len(features_ok)}. Archivo: {CONFIG['output_file']}")
    logging.info(f"Total features con error: {len(features_error)}.")

if __name__ == "__main__":

    main()
