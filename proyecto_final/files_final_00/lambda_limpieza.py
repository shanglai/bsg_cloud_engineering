import json
import os
import csv
import io
import unicodedata
import boto3

# ETL de la blacklist: lee el CSV CRUDO de S3, lo limpia, y escribe el CSV LIMPIO.
# Este es el paso de preparacion de datos antes de que la Lambda de validacion lo use.
#
# Limpieza que aplica:
#   - quita espacios extra y normaliza a un solo espacio
#   - pasa a minusculas para comparacion consistente
#   - quita acentos (Gomez == Gómez)
#   - detecta y quita duplicados (por id_sancion)
#   - descarta filas sin nombre
#   - se queda solo con las columnas utiles (quita notas_internas)

BUCKET = os.environ.get("BUCKET_BLACKLIST", "")
KEY_CRUDO = os.environ.get("KEY_CRUDO", "raw/blacklist_cruda.csv")
KEY_LIMPIO = os.environ.get("KEY_LIMPIO", "blacklist.csv")

s3 = boto3.client("s3")


def quitar_acentos(texto):
    """Convierte 'Gómez' -> 'Gomez' para comparacion consistente."""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalizar_nombre(nombre):
    """Normaliza un nombre: sin acentos, minusculas, un solo espacio, sin espacios extra."""
    if not nombre:
        return ""
    limpio = quitar_acentos(nombre)
    limpio = " ".join(limpio.strip().lower().split())
    return limpio


def lambda_handler(event, context):
    # 1. Leer el CSV crudo de S3
    obj = s3.get_object(Bucket=BUCKET, Key=KEY_CRUDO)
    contenido = obj["Body"].read().decode("utf-8")
    lector = csv.DictReader(io.StringIO(contenido))

    filas_limpias = []
    ids_vistos = set()
    descartadas = {"sin_nombre": 0, "duplicadas": 0}

    for fila in lector:
        nombre = normalizar_nombre(fila.get("nombre", ""))
        id_sancion = (fila.get("id_sancion", "") or "").strip()

        # descartar filas sin nombre
        if not nombre:
            descartadas["sin_nombre"] += 1
            continue

        # descartar duplicados (por id_sancion)
        if id_sancion in ids_vistos:
            descartadas["duplicadas"] += 1
            continue
        ids_vistos.add(id_sancion)

        # quedarnos solo con las columnas utiles (sin notas_internas)
        filas_limpias.append({
            "nombre": nombre,
            "id_sancion": id_sancion,
            "fuente": (fila.get("fuente", "") or "").strip(),
        })

    # 2. Escribir el CSV limpio a S3
    salida = io.StringIO()
    escritor = csv.DictWriter(salida, fieldnames=["nombre", "id_sancion", "fuente"])
    escritor.writeheader()
    escritor.writerows(filas_limpias)

    s3.put_object(
        Bucket=BUCKET,
        Key=KEY_LIMPIO,
        Body=salida.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    # 3. Devolver un resumen (util para Step Functions y para logs)
    resumen = {
        "filas_entrada": len(filas_limpias) + descartadas["sin_nombre"] + descartadas["duplicadas"],
        "filas_limpias": len(filas_limpias),
        "descartadas_sin_nombre": descartadas["sin_nombre"],
        "descartadas_duplicadas": descartadas["duplicadas"],
        "archivo_salida": f"s3://{BUCKET}/{KEY_LIMPIO}",
    }
    print("ETL completado:", json.dumps(resumen))
    return resumen
