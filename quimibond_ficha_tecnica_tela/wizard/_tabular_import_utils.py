# -*- coding: utf-8 -*-
"""Utilidades compartidas para los wizards de importación tabular
(fila por fila) de fichas técnicas desde Excel.
"""
import unicodedata


def normalize_header(text):
    """Normaliza un encabezado de columna para hacer el match tolerante a
    acentos, mayúsculas/minúsculas y espacios extra.
    ej. 'Rendimiento (mts/kg)' -> 'rendimiento mts kg'
    """
    if text is None:
        return ''
    text = str(text).strip().lower()
    text = ''.join(
        ch for ch in unicodedata.normalize('NFKD', text)
        if not unicodedata.combining(ch)
    )
    cleaned = []
    for ch in text:
        cleaned.append(ch if (ch.isalnum() or ch.isspace()) else ' ')
    text = ''.join(cleaned)
    return ' '.join(text.split())


def build_header_index(header_row, header_map):
    """Dado la primera fila de un Excel (lista de valores de celda) y un
    diccionario {alias_normalizado: nombre_tecnico_de_campo}, regresa un
    diccionario {indice_de_columna: nombre_tecnico_de_campo}.

    Columnas cuyo encabezado no matchee ningún alias se ignoran (permite
    columnas extra/informativas en el archivo sin que truene la importación).
    """
    col_index = {}
    for idx, cell_value in enumerate(header_row):
        key = normalize_header(cell_value)
        if key in header_map:
            col_index[idx] = header_map[key]
    return col_index


def clean_num(value):
    if value in (None, '', 'N/A', 'NA', 'n/a'):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        digits = ''.join(ch for ch in str(value) if (ch.isdigit() or ch in '.-'))
        return float(digits) if digits else 0.0
    except ValueError:
        return 0.0


def row_is_empty(row_values):
    return all(v is None or str(v).strip() == '' for v in row_values)
