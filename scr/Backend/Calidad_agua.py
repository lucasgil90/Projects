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
    "output_dir": None,
    "state_dir": None,
    "max_workers": 12,
    "pause_geocode": 0.0,
    "base_url": "https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/informacionAbastecimientoActionCA.do?idRed=",
    "zip_range": (1, 25500)
}

def setup_environment():
    CONFIG["output_dir"] = os.environ.get("OUTPUT_DIR")
    CONFIG["state_dir"] = os.environ.get("STATE_DIR")

    if not CONFIG["output_dir"] or not CONFIG["state_dir"]:
        raise ValueError("Las variables de entorno 'OUTPUT_DIR' y 'STATE_DIR' deben estar configuradas.")

    CONFIG["output_file"] = os.path.join(CONFIG["output_dir"], "abastecimientos_test.geojson")
    CONFIG["error_file"] = os.path.join(CONFIG["state_dir"], "abastecimientos_geocode_errors.json")
    CONFIG["cache_file"] = os.path.join(CONFIG["state_dir"], "geocode_cache.json")
    CONFIG["progress_file"] = os.path.join(CONFIG["state_dir"], "progress.json")
    CONFIG["log_file_prefix"] = os.path.join(CONFIG["state_dir"], "geocoding_")

    os.makedirs(CONFIG["output_dir"], exist_ok=True)
    os.makedirs(CONFIG["state_dir"], exist_ok=True)

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

setup_environment()

def clean_text(text):
    return " ".join(text.strip().split())

# --- Manejo de caché ---
if os.path.exists(CONFIG["cache_file"]):
    try:
        with open(CONFIG["cache_file"], 'r', encoding='utf-8') as f:
            geocode_cache = json.load(f)
        logging.info(f"Se ha cargado el caché de geocodificación con {len(geocode_cache)} entradas.")
    except json.JSONDecodeError:
        logging.warning("El archivo de caché está corrupto o vacío. Se creará un nuevo caché.")
        geocode_cache = {}
else:
    geocode_cache = {}
    logging.info("No se encontró archivo de caché, comenzando con caché vacío.")

def geocode_with_retry(localidad, idred, retries=3, backoff_factor=0.5):
    cache_key = f"{idred}::{localidad}"
    if cache_key in geocode_cache:
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
                logging.warning(f"Geocodificación vacía para '{localidad}' (idRed={idred}).")
                return [0.0, 0.0]
        except (requests.exceptions.RequestException, IndexError, ValueError) as e:
            logging.warning(f"Intento {i+1}/{retries} fallido para '{localidad}' (idRed={idred}): {e}")
            time.sleep(backoff_factor * (2 ** i))

    logging.error(f"Geocodificación fallida para '{localidad}' (idRed={idred}) después de {retries} intentos.")
    return [0.0, 0.0]

# --- Sesión de requests ---
session = requests.Session()
session.headers.update({
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
})

def obtener_y_procesar_datos(idred):
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
        if comentario_tag:
            comentario_text = comentario_tag.text.strip()
            comentario = clean_text(comentario_text) if comentario_text else "-"
        else:
            comentario = "-"
        for loc in localidades:
            coords = geocode_with_retry(loc, idred)
            time.sleep(CONFIG['pause_geocode'])

            properties = {
                "Gestor": gestor,
                "Localidad": loc,
                "Calidad del agua": calidad,
                "Comentario": comentario,
                "idRed": idred,
                "denominacion": denominacion
            }
            feature = {"type": "Feature", "properties": properties, "geometry": {"type": "Point", "coordinates": coords}}

            if coords == [0.0, 0.0]:
                features_error.append(feature)
            else:
                features_ok.append(feature)

        return features_ok, features_error

    except (requests.exceptions.RequestException, Exception) as e:
        logging.error(f"Error al procesar idRed={idred}: {e}")
        return [], []

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

    lista_zip = list(map(int, np.arange(CONFIG["zip_range"][0], CONFIG["zip_range"][1], 1)))

    # Procesar siempre los que ya fueron válidos una vez
    ids_a_procesar = list(procesados) if procesados else lista_zip
    total_ids = len(ids_a_procesar)

    if total_ids > 0:
        logging.info(f"Comenzando a procesar {total_ids} IDs.")
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
            futuros = {executor.submit(obtener_y_procesar_datos, idred): idred for idred in ids_a_procesar}

            for futuro in as_completed(futuros):
                idred = futuros[futuro]
                try:
                    nuevas_features_ok, nuevas_features_error = futuro.result()
                    if nuevas_features_ok:
                        features_ok.extend(nuevas_features_ok)
                        procesados.add(idred)  # Confirmar como válido si devuelve algo
                    if nuevas_features_error:
                        features_error.extend(nuevas_features_error)

                    with open(CONFIG["progress_file"], 'w', encoding='utf-8') as f:
                        json.dump(list(procesados), f, ensure_ascii=False)

                    avance = (len(procesados) / len(lista_zip)) * 100
                    logging.info(f"AVANCE: ID {idred} procesado. Progreso: {len(procesados)}/{len(lista_zip)} ({avance:.2f}%)")

                except Exception as e:
                    logging.error(f"Error procesando idRed={idred}: {e}")

    geojson_ok = {"type": "FeatureCollection", "features": features_ok}
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(geojson_ok, f, ensure_ascii=False, indent=4)

    if features_error:
        with open(CONFIG["error_file"], "w", encoding="utf-8") as f:
            json.dump(features_error, f, ensure_ascii=False, indent=4)
        logging.warning(f"Se han guardado {len(features_error)} errores de geocodificación en '{CONFIG['error_file']}'.")

    with open(CONFIG["cache_file"], 'w', encoding='utf-8') as f:
        json.dump(geocode_cache, f, ensure_ascii=False, indent=4)

    logging.info(f"Proceso completado. Features OK: {len(features_ok)}. Archivo: {CONFIG['output_file']}")
    logging.info(f"Total errores de coordenadas: {len(features_error)}.")

if __name__ == "__main__":
    main()
