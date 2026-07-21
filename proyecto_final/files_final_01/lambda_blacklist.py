import json
import os
import csv
import io
import boto3

# La Lambda lee la blacklist desde S3 y valida si un nombre esta en ella.
# Diseno: separacion clara entre datos (S3) y logica (esta funcion).

BUCKET = os.environ.get("BUCKET_BLACKLIST", "")
KEY = os.environ.get("KEY_BLACKLIST", "blacklist.csv")

s3 = boto3.client("s3")


def cargar_blacklist():
    """Lee el CSV de blacklist desde S3 y devuelve un set de nombres normalizados."""
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    contenido = obj["Body"].read().decode("utf-8")
    lector = csv.DictReader(io.StringIO(contenido))
    # normalizamos a minusculas y sin espacios extra para comparar
    return {fila["nombre"].strip().lower(): fila for fila in lector}


def normalizar(nombre):
    """Normaliza un nombre para comparacion: minusculas, sin espacios extra."""
    return " ".join((nombre or "").strip().lower().split())


def lambda_handler(event, context):
    # Headers CORS: el front (en Cloud Run) llama a esta API desde otro origen
    cors = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "content-type,x-api-key",
        "Access-Control-Allow-Methods": "POST,OPTIONS",
    }

    # Preflight CORS
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors, "body": ""}

    try:
        body = json.loads(event.get("body") or "{}")
        nombre = body.get("nombre", "")

        if not nombre:
            return {
                "statusCode": 400,
                "headers": cors,
                "body": json.dumps({"error": "falta el campo 'nombre'"}),
            }

        # Cargar la lista y validar
        blacklist = cargar_blacklist()
        nombre_norm = normalizar(nombre)

        if nombre_norm in blacklist:
            registro = blacklist[nombre_norm]
            resultado = {
                "nombre": nombre,
                "en_blacklist": True,
                "id_sancion": registro.get("id_sancion", ""),
                "fuente": registro.get("fuente", ""),
                "mensaje": "Persona en lista de sancionados. Solicitud rechazada.",
            }
        else:
            resultado = {
                "nombre": nombre,
                "en_blacklist": False,
                "mensaje": "Persona no sancionada. Puede continuar al scoring.",
            }

        return {"statusCode": 200, "headers": cors, "body": json.dumps(resultado, ensure_ascii=False)}

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": cors,
            "body": json.dumps({"error": str(e)}),
        }
