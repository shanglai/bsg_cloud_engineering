"""
Pipeline de Apache Beam - Dataset Wine (UCI)
Programa Python Certified Data Engineer - BSG Institute

QUE HACE ESTE PIPELINE:
  1. Lee el archivo wine.csv desde un bucket de Cloud Storage.
  2. Normaliza las features numericas (escala min-max a rango 0..1).
  3. En una rama aparte, calcula estadisticas de resumen POR TIPO de vino.
  4. Escribe ambos resultados de vuelta al bucket.

COMO LO USAS (alumno):
  - Busca la seccion marcada con  "### TU CAMBIO AQUI ###".
  - Ahi eliges sobre que columna calcular las estadisticas de resumen.
  - Prueba local (DirectRunner) si quieres, y luego SUBE este archivo .py
    al bucket de entregas con tu nombre, como te indica el notebook.

  El instructor lo recoge y lo corre con Dataflow en su cuenta.

PARAMETROS (los pasa el instructor al ejecutar):
  --input   gs://BUCKET/wine.csv
  --output  gs://BUCKET/salidas/NOMBRE/
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# ---------------------------------------------------------------------------
# El dataset Wine de UCI no tiene encabezado. La primera columna es el TIPO
# de vino (1, 2 o 3) y las otras 13 son atributos quimicos.
# ---------------------------------------------------------------------------
COLUMNAS = [
    "type", "alcohol", "malic", "ash", "alcalinity", "magnesium",
    "phenols", "flavanoids", "nonflavanoids", "proanthocyanins",
    "color", "hue", "dilution", "proline",
]

# Rangos min-max conocidos del dataset Wine, para normalizar a 0..1.
# (En un caso real se calcularian; aqui los fijamos para mantener el lab simple.)
RANGOS = {
    "alcohol": (11.03, 14.83), "malic": (0.74, 5.80), "ash": (1.36, 3.23),
    "alcalinity": (10.6, 30.0), "magnesium": (70, 162), "phenols": (0.98, 3.88),
    "flavanoids": (0.34, 5.08), "nonflavanoids": (0.13, 0.66),
    "proanthocyanins": (0.41, 3.58), "color": (1.28, 13.0),
    "hue": (0.48, 1.71), "dilution": (1.27, 4.0), "proline": (278, 1680),
}

# ===========================================================================
# ###  TU CAMBIO AQUI  ###
# Elige sobre QUE columna quieres las estadisticas de resumen por tipo de vino.
# Opciones validas: cualquiera de las llaves de RANGOS (ej. "alcohol",
# "color", "proline", "flavanoids", ...).
# Cambia el valor de abajo por la columna que prefieras analizar.
# ===========================================================================
COLUMNA_RESUMEN = "alcohol"
# ===========================================================================


def parsear_linea(linea: str) -> dict:
    """Convierte una linea CSV cruda en un diccionario con nombres de columna."""
    partes = linea.strip().split(",")
    registro = {}
    for nombre, valor in zip(COLUMNAS, partes):
        if nombre == "type":
            registro[nombre] = int(float(valor))
        else:
            registro[nombre] = float(valor)
    return registro


def normalizar(registro: dict) -> dict:
    """Escala cada feature numerica a rango 0..1 con min-max."""
    salida = {"type": registro["type"]}
    for col, (mn, mx) in RANGOS.items():
        valor = registro[col]
        salida[col] = round((valor - mn) / (mx - mn), 4) if mx > mn else 0.0
    return salida


def a_csv(registro: dict) -> str:
    """Convierte un registro normalizado de vuelta a linea CSV."""
    return ",".join(str(registro[c]) for c in COLUMNAS)


def estadisticas_por_tipo(elementos) -> str:
    """
    Recibe (tipo, [lista de valores de la columna elegida]) y devuelve una
    linea de resumen: tipo, conteo, promedio, minimo, maximo.
    """
    tipo, valores = elementos
    valores = list(valores)
    n = len(valores)
    promedio = sum(valores) / n if n else 0
    return f"{tipo},{n},{round(promedio, 4)},{round(min(valores), 4)},{round(max(valores), 4)}"


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Ruta del wine.csv (local o gs://)")
    parser.add_argument("--output", required=True, help="Carpeta de salida (local o gs://)")
    args, beam_args = parser.parse_known_args(argv)

    opciones = PipelineOptions(beam_args)

    with beam.Pipeline(options=opciones) as p:
        # EXTRACT: leer el CSV y parsear cada linea a diccionario
        registros = (
            p
            | "Leer CSV"   >> beam.io.ReadFromText(args.input)
            | "Parsear"    >> beam.Map(parsear_linea)
        )

        # RAMA 1 - TRANSFORM: normalizar y escribir
        (
            registros
            | "Normalizar"      >> beam.Map(normalizar)
            | "A CSV"           >> beam.Map(a_csv)
            | "Escribir norm"   >> beam.io.WriteToText(
                args.output + "wine_normalizado",
                file_name_suffix=".csv",
                header=",".join(COLUMNAS),
            )
        )

        # RAMA 2 - ESTADISTICAS de resumen por tipo de vino
        # sobre la columna que elegiste en COLUMNA_RESUMEN
        (
            registros
            | "Tipo y valor" >> beam.Map(lambda r: (r["type"], r[COLUMNA_RESUMEN]))
            | "Agrupar"      >> beam.GroupByKey()
            | "Resumir"      >> beam.Map(estadisticas_por_tipo)
            | "Escribir stats" >> beam.io.WriteToText(
                args.output + f"resumen_{COLUMNA_RESUMEN}_por_tipo",
                file_name_suffix=".csv",
                header=f"tipo,conteo,promedio_{COLUMNA_RESUMEN},minimo,maximo",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
