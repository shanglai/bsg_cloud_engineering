"""
Servicio de scoring y guardado para el proceso de credito.
Dos endpoints:
  POST /scoring  -> evalua salario y score, decide aprobado y monto
  POST /guardar  -> guarda la solicitud firmada en BigQuery

Diseno: separacion clara entre la LOGICA de negocio (deterministica, aqui)
y el almacenamiento (BigQuery). El scoring es una regla simple y auditable.
"""
import os
import json
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery

app = Flask(__name__)
CORS(app)  # permite que el front (otro origen) llame a este servicio

# Config de BigQuery (via variables de entorno)
PROJECT_ID = os.environ.get("GCP_PROJECT", "")
DATASET = os.environ.get("BQ_DATASET", "credito")
TABLA = os.environ.get("BQ_TABLA", "solicitudes")

# Umbrales de la regla de negocio (parametrizables)
SALARIO_MINIMO = float(os.environ.get("SALARIO_MINIMO", "10000"))
SCORE_MINIMO = float(os.environ.get("SCORE_MINIMO", "700"))


def calcular_scoring(salario, score):
    """
    Regla de otorgamiento (deterministica y auditable):
      - Aprobado si salario > 10000 Y score > 700
      - Monto de credito = salario * score / 1000
    """
    aprobado = salario > SALARIO_MINIMO and score > SCORE_MINIMO

    if aprobado:
        monto = round(salario * score / 1000, 2)
        mensaje = "Cumple los criterios: salario y score suficientes."
    else:
        monto = 0.0
        # explicar POR QUE no fue aprobado (transparencia)
        razones = []
        if salario <= SALARIO_MINIMO:
            razones.append(f"salario no supera {SALARIO_MINIMO:.0f}")
        if score <= SCORE_MINIMO:
            razones.append(f"score no supera {SCORE_MINIMO:.0f}")
        mensaje = "No aprobado: " + " y ".join(razones) + "."

    return {"aprobado": aprobado, "monto_credito": monto, "mensaje": mensaje}


@app.route("/scoring", methods=["POST"])
def scoring():
    """Evalua una solicitud y devuelve la decision. No guarda nada aun."""
    try:
        datos = request.get_json(force=True) or {}
        salario = float(datos.get("salario", 0))
        score = float(datos.get("score", 0))

        resultado = calcular_scoring(salario, score)
        return jsonify(resultado), 200
    except (ValueError, TypeError) as e:
        return jsonify({"error": f"datos invalidos: {e}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/guardar", methods=["POST"])
def guardar():
    """Guarda la solicitud firmada en BigQuery."""
    try:
        datos = request.get_json(force=True) or {}

        # Fila a insertar. Coincide con el esquema de la tabla.
        fila = {
            "nombre": datos.get("nombre", ""),
            "salario": float(datos.get("salario", 0)),
            "score": float(datos.get("score", 0)),
            "aprobado": bool(datos.get("aprobado", False)),
            "monto_credito": float(datos.get("monto_credito", 0)),
            "firma_nombre": datos.get("firma_nombre", ""),
            "firma_timestamp": datos.get("firma_timestamp", datetime.now(timezone.utc).isoformat()),
            "contrato_hash": datos.get("contrato_hash", ""),
            "firmado": bool(datos.get("firmado", False)),
            "registrado_en": datetime.now(timezone.utc).isoformat(),
        }

        client = bigquery.Client(project=PROJECT_ID)
        tabla_ref = f"{PROJECT_ID}.{DATASET}.{TABLA}"
        errores = client.insert_rows_json(tabla_ref, [fila])

        if errores:
            return jsonify({"error": "no se pudo guardar", "detalle": errores}), 500

        return jsonify({"ok": True, "mensaje": "Solicitud guardada en BigQuery", "fila": fila}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/", methods=["GET"])
def salud():
    """Healthcheck simple."""
    return jsonify({"servicio": "scoring-credito", "estado": "activo"}), 200


if __name__ == "__main__":
    # Cloud Run inyecta el puerto en PORT (default 8080)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
