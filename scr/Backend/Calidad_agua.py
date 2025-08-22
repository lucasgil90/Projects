import requests # Importa la biblioteca para hacer peticiones HTTP.
from bs4 import BeautifulSoup # Importa Beautiful Soup para parsear HTML.
import json # Importa la biblioteca para trabajar con datos JSON.
import time # Importa la biblioteca para manejar pausas y tiempo.
import numpy as np # Importa NumPy para operaciones numéricas, en este caso para crear un rango de IDs.
import os # Importa el módulo 'os' para interactuar con el sistema operativo, leer variables de entorno y manipular rutas de archivos.
import logging # Importa la biblioteca para registrar mensajes de estado, advertencias y errores.
from concurrent.futures import ThreadPoolExecutor, as_completed # Importa clases para ejecutar tareas en hilos de forma concurrente.
from datetime import datetime # Importa la clase datetime para trabajar con fechas y horas.

# --- Configuración del Script ---
CONFIG = {
    # Estos valores se inicializan como None y se asignarán a partir de las variables de entorno.
    "output_dir": None,
    "state_dir": None,
    "max_workers": 12, # Número máximo de hilos para ejecutar en paralelo.
    "pause_geocode": 0.0, # Pausa en segundos entre cada llamada a la API de geocodificación para evitar bloqueos.
    "base_url": "https://sinac.sanidad.gob.es/CiudadanoWeb/ciudadano/informacionAbastecimientoActionCA.do?idRed=", # URL base para el scraping.
    "zip_range": (1, 25500) # Rango de IDs (códigos postales) a procesar. Total 25500
}

def setup_environment():
    """Configura las rutas y el logger basándose en las variables de entorno."""

    # Leer las variables de entorno para las rutas de salida y estado.
    CONFIG["output_dir"] = os.environ.get("OUTPUT_DIR") # Lee el directorio de salida del GeoJSON desde el entorno.
    CONFIG["state_dir"] = os.environ.get("STATE_DIR") # Lee el directorio para los archivos de estado desde el entorno.

    # Validación crítica: comprueba si las variables de entorno están definidas.
    if not CONFIG["output_dir"] or not CONFIG["state_dir"]:
        # Si no se encuentran las variables, lanza un error para detener la ejecución.
        raise ValueError("Las variables de entorno 'OUTPUT_DIR' y 'STATE_DIR' deben estar configuradas para que el script funcione.")

    # Construye las rutas completas para todos los archivos a partir de los directorios base.
    CONFIG["output_file"] = os.path.join(CONFIG["output_dir"], "abastecimientos_test.geojson") # Ruta completa del GeoJSON de salida.
    CONFIG["error_file"] = os.path.join(CONFIG["output_dir"], "abastecimientos_errors.geojson") # Ruta completa para los GeoJSON con errores.
    CONFIG["cache_file"] = os.path.join(CONFIG["state_dir"], "geocode_cache.json") # Ruta completa para el archivo de caché de geocodificación.
    CONFIG["progress_file"] = os.path.join(CONFIG["state_dir"], "progress.json") # Ruta completa para el archivo de progreso.
    CONFIG["log_file_prefix"] = os.path.join(CONFIG["state_dir"], "geocoding_") # Prefijo para los archivos de log.

    # Asegura que ambos directorios (salida y estado) existan.
    os.makedirs(CONFIG["output_dir"], exist_ok=True) # Crea el directorio de salida si no existe.
    os.makedirs(CONFIG["state_dir"], exist_ok=True) # Crea el directorio de estado si no existe.

    # Configuración del logger
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") # Obtiene la fecha y hora actual para el nombre del log.
    log_file = f"{CONFIG['log_file_prefix']}{fecha_hora}_test.log" # Crea el nombre completo del archivo de log.
    logging.basicConfig(
        level=logging.INFO, # Nivel de registro: INFO, WARN, ERROR, etc.
        format="%(asctime)s [%(levelname)s] %(message)s", # Formato de los mensajes del log.
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"), # Un handler para escribir en un archivo.
            logging.StreamHandler() # Un handler para mostrar en la consola.
        ]
    )

setup_environment() # Llama a la función de configuración para que se ejecute al inicio del script.

# --- Funciones de utilidad ---
def clean_text(text):
    """Limpia el texto eliminando espacios extra."""
    return " ".join(text.strip().split())

# --- Manejo de caché ---
if os.path.exists(CONFIG["cache_file"]): # Comprueba si el archivo de caché existe.
    try:
        with open(CONFIG["cache_file"], 'r', encoding='utf-8') as f:
            geocode_cache = json.load(f) # Carga el caché si el archivo existe.
        logging.info(f"Se ha cargado el caché de geocodificación con {len(geocode_cache)} entradas.")
    except json.JSONDecodeError:
        logging.warning("El archivo de caché está corrupto o vacío. Se creará un nuevo caché.")
        geocode_cache = {} # Si el archivo no es un JSON válido, se inicializa un caché vacío.
else:
    geocode_cache = {} # Inicializa un caché vacío si el archivo no existe.
    logging.info("No se encontró archivo de caché, comenzando con caché vacío.")

def geocode_with_retry(localidad, idred, retries=3, backoff_factor=0.5):
    """Intenta geocodificar una localidad con reintentos en caso de fallo de red."""
    cache_key = f"{idred}::{localidad}" # Crea una clave única para la localidad y el ID.
    if cache_key in geocode_cache: # Comprueba si la geocodificación ya está en el caché.
        logging.debug(f"Coordenadas de '{localidad}' (idRed={idred}) recuperadas del caché.")
        return geocode_cache[cache_key] # Devuelve las coordenadas del caché.

    url = "https://nominatim.openstreetmap.org/search" # URL de la API de geocodificación.
    params = {"format": "json", "q": f"{localidad}, España"} # Parámetros de la petición.
    headers = {"User-Agent": "GeoJSON-Extractor/1.0"} # Encabezado para identificar la petición.

    for i in range(retries): # Bucle de reintentos.
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10) # Hace la petición a la API.
            r.raise_for_status() # Lanza un error si la respuesta HTTP no es exitosa.
            data = r.json() # Parsea la respuesta JSON.
            if data:
                lat = float(data[0]["lat"]) # Extrae la latitud.
                lon = float(data[0]["lon"]) # Extrae la longitud.
                coords = [lon, lat]
                
                # ➡️ Modificación: solo guarda en caché si la geocodificación fue exitosa.
                geocode_cache[cache_key] = coords # Guarda las coordenadas en el caché.
                return coords
            else:
                logging.warning(f"Geocodificación fallida para '{localidad}' (idRed={idred}): Respuesta vacía.")
                # ➡️ No se guarda en caché si falla.
                return [0.0, 0.0] # Devuelve coordenadas nulas en caso de fallo.
        except (requests.exceptions.RequestException, IndexError, ValueError) as e:
            logging.warning(f"Intento {i+1}/{retries} fallido para '{localidad}' (idRed={idred}): {e}")
            time.sleep(backoff_factor * (2 ** i)) # Espera antes de reintentar (backoff exponencial).
    
    logging.error(f"Geocodificación fallida para '{localidad}' (idRed={idred}) después de {retries} intentos. No se añade al caché.")
    # ➡️ No se guarda en caché si falla.
    return [0.0, 0.0] # Devuelve coordenadas nulas si todos los reintentos fallan.

# --- Sesión de requests ---
session = requests.Session() # Crea una sesión de requests para reutilizar la conexión.
session.headers.update({
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
})

def obtener_y_procesar_datos(idred):
    """Obtiene los datos de la web de SINAC y construye las features."""
    features_ok = [] # Lista para almacenar las features que se geocodificaron correctamente.
    features_error = [] # Lista para almacenar las features con errores de geocodificación.
    url = f"{CONFIG['base_url']}{idred}" # Construye la URL completa.
    
    try:
        resp = session.get(url, timeout=10) # Realiza la petición GET.
        resp.raise_for_status() # Lanza un error si la respuesta no es 2xx.
        soup = BeautifulSoup(resp.text, "html.parser") # Parsea el HTML de la respuesta.

        if not soup.find("th", text="Denominación"): # Comprueba si la página tiene datos.
            logging.debug(f"idRed={idred} no contiene datos.")
            return [], [] # Devuelve listas vacías si no hay datos.

        denominacion = clean_text(soup.find("th", text="Denominación").find_next("td").text) # Extrae la denominación.
        gestor = clean_text(soup.find("th", text="Gestor").find_next("td").text) # Extrae el gestor.
        localidades_html = soup.find("th", text="Localidades abastecidas").find_next("td") # Busca el tag de localidades.
        localidades = [clean_text(loc) for loc in localidades_html.stripped_strings] # Extrae las localidades.
        calidad = clean_text(soup.find("th", text="Calidad del agua").find_next("td").text) # Extrae la calidad del agua.
        comentario_tag = soup.find("th", text="Comentario Aut. Sanitaria") # Busca el tag de comentario.
        if comentario_tag:
            comentario_text = comentario_tag.text.strip()
            comentario = clean_text(comentario_text) if comentario_text else "-"
        else:
            comentario = "-"
        for loc in localidades: # Itera sobre cada localidad.
            coords = geocode_with_retry(loc, idred) # Geocodifica la localidad.
            time.sleep(CONFIG['pause_geocode']) # Pausa para no sobrecargar la API.
            
            properties = { # Crea el diccionario de propiedades de la feature.
                "Gestor": gestor,
                "Localidad": loc,
                "Calidad del agua": calidad,
                "Comentario": comentario
            }
            feature = {"type": "Feature", "properties": properties, "geometry": {"type": "Point", "coordinates": coords}} # Construye la feature.
            
            if coords == [0.0, 0.0]:
                feature["properties"]["idRed"] = idred
                feature["properties"]["denominacion"] = denominacion
                features_error.append(feature) # Añade la feature a la lista de errores si la geocodificación falló.
            else:
                features_ok.append(feature) # Añade la feature a la lista de éxito.
        
        return features_ok, features_error # Devuelve ambas listas.

    except (requests.exceptions.RequestException, Exception) as e:
        logging.error(f"Error al procesar idRed={idred}: {e}")
        return [], [] # En caso de cualquier error, devuelve listas vacías.

# --- Lógica principal de ejecución ---
def main():
    if os.path.exists(CONFIG["progress_file"]): # Comprueba si el archivo de progreso existe.
        with open(CONFIG["progress_file"], 'r', encoding='utf-8') as f:
            procesados = set(json.load(f)) # Carga los IDs ya procesados.
        logging.info(f"Se han cargado {len(procesados)} IDs procesados previamente.")
    else:
        procesados = set() # Crea un set vacío si el archivo no existe.
        logging.info("No se encontró archivo de progreso, comenzando desde cero.")

    features_ok = [] # Lista para almacenar las features de éxito.
    features_error = [] # Lista para almacenar las features con errores.
    
    if os.path.exists(CONFIG["output_file"]): # Comprueba si el archivo de salida existe.
        try:
            with open(CONFIG["output_file"], 'r', encoding='utf-8') as f:
                features_ok = json.load(f)["features"] # Carga las features existentes.
            logging.info(f"Se han cargado {len(features_ok)} features del archivo de salida existente.")
        except json.JSONDecodeError:
            logging.warning("El archivo de salida está corrupto o vacío. Se creará un nuevo archivo.")
            features_ok = []

    if os.path.exists(CONFIG["error_file"]): # Comprueba si el archivo de errores existe.
        try:
            with open(CONFIG["error_file"], 'r', encoding='utf-8') as f:
                features_error = json.load(f)["features"] # Carga las features con errores.
            logging.info(f"Se han cargado {len(features_error)} features del archivo de errores existente.")
        except json.JSONDecodeError:
            logging.warning("El archivo de errores está corrupto o vacío. Se creará un nuevo archivo.")
            features_error = []

    lista_zip = list(map(int, np.arange(CONFIG["zip_range"][0], CONFIG["zip_range"][1], 1))) # Crea una lista de IDs a procesar.
    ids_a_procesar = [idred for idred in lista_zip if idred not in procesados] # Filtra los IDs ya procesados.
    total_ids = len(lista_zip) # Total de IDs en el rango.
    total_a_procesar = len(ids_a_procesar) # IDs que se procesarán en esta ejecución.

    if total_a_procesar > 0: # Si hay IDs nuevos por procesar...
        logging.info(f"Comenzando a procesar {total_a_procesar} IDs restantes de un total de {total_ids}.")
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor: # Inicia el pool de hilos.
            futuros = {executor.submit(obtener_y_procesar_datos, idred): idred for idred in ids_a_procesar} # Envía tareas al pool.

            for futuro in as_completed(futuros): # Espera a que cada tarea termine.
                idred = futuros[futuro] # Obtiene el ID de la tarea completada.
                try:
                    nuevas_features_ok, nuevas_features_error = futuro.result() # Obtiene el resultado de la tarea.
                    if nuevas_features_ok:
                        features_ok.extend(nuevas_features_ok) # Agrega las features de éxito.
                    if nuevas_features_error:
                        features_error.extend(nuevas_features_error) # Agrega las features con errores.
                    
                    procesados.add(idred) # Añade el ID a la lista de procesados.
                    with open(CONFIG["progress_file"], 'w', encoding='utf-8') as f:
                        json.dump(list(procesados), f, ensure_ascii=False) # Guarda el progreso.
                    
                    avance = (len(procesados) / total_ids) * 100 # Calcula el porcentaje de avance.
                    logging.info(f"AVANCE: Se ha procesado el ID {idred}. Progreso total: {len(procesados)}/{total_ids} ({avance:.2f}%)")
                
                except Exception as e:
                    logging.error(f"Error procesando el resultado del futuro para idRed={idred}: {e}")

    geojson_ok = {"type": "FeatureCollection", "features": features_ok} # Construye el GeoJSON final.
    with open(CONFIG["output_file"], "w", encoding="utf-8") as f:
        json.dump(geojson_ok, f, ensure_ascii=False, indent=4) # Guarda el GeoJSON en el archivo de salida.

    if features_error: # Si hay features con errores...
        geojson_error = {"type": "FeatureCollection", "features": features_error} # Construye el GeoJSON de errores.
        with open(CONFIG["error_file"], "w", encoding="utf-8") as f:
            json.dump(geojson_error, f, ensure_ascii=False, indent=4) # Guarda el GeoJSON de errores.
        logging.warning(f"Se han guardado {len(features_error)} features con errores de geocodificación en '{CONFIG['error_file']}'.")

    with open(CONFIG["cache_file"], 'w', encoding='utf-8') as f:
        json.dump(geocode_cache, f, ensure_ascii=False, indent=4) # Guarda el caché de geocodificación actualizado.

    logging.info(f"Proceso completado. Total features OK: {len(features_ok)}. Archivo: {CONFIG['output_file']}")
    logging.info(f"Total features con error: {len(features_error)}.")

if __name__ == "__main__":
    main() # Inicia el script si se ejecuta directamente.





