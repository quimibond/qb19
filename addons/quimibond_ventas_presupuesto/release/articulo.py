# -*- coding: utf-8 -*-
"""Artículo comercial y datos de la tela que salen del código — Python puro.

Regla de Dirección (2026-11-04, DAT P-D02-01): **con «I» al frente es kilos,
sin «I» es metros**, cada producto en una sola unidad. La misma tela se vende
con hasta tres códigos: el de metros (WJ045Q22JNT160), el de kilos con «I»
(IWJ045Q22JNT160) y la versión en m² con «M2» al final (WJ045Q22JNT160M2).
El artículo que los junta para medir es el de metros.

Estas funciones solo leen el código; el módulo de Fichas técnicas manda
cuando la tela tiene ficha (``qb_articulo_id`` y ``qb_m_por_kg`` en
``product.product``, diseño v3 sección 2.2).
"""
import re

# Posiciones de la DAT P-D02-01 (la misma de qb_costeo/models/horas.py:64):
# composición, dibujo, gramaje (3), tipo de hilo, galga (2), proceso (H/I/J),
# color (2), ancho en cm (3).
NOMEN_RE = re.compile(r'^I?([A-Z])([A-Z])(\d{3})([QBR])(\d{2})([HIJ])([A-Z]{2})(\d{3})')
PROCESS_POSITION = 8  # índice 0 de la posición 9 (H, I o J)
PROCESS_CODES = ('H', 'I', 'J')
KG_PREFIX = 'I'
M2_SUFFIX = 'M2'
YARD_M = 0.9144


def clean(code):
    return re.sub(r'\s+', '', (code or '').upper()).rstrip('.')


def es_codigo_tela(code):
    """14 o 16 posiciones con H, I o J en la posición 9 (crudo, teñido, acabado)."""
    code = clean(code)
    return len(code) in (14, 16) and code[PROCESS_POSITION:PROCESS_POSITION + 1] in PROCESS_CODES


def es_codigo_kg(code):
    """La tela en kilos: «I» al frente de un código de tela (IWJ045Q22JNT160)."""
    code = clean(code)
    return code.startswith(KG_PREFIX) and es_codigo_tela(code[1:])


def codigo_metros(code):
    """El código del artículo en metros: quita la «I» de kilos y el «M2» final.
    Devuelve el mismo código si no aplica ninguna de las dos reglas."""
    code = clean(code)
    if code.endswith(M2_SUFFIX) and es_codigo_tela(code[:-len(M2_SUFFIX)]):
        code = code[:-len(M2_SUFFIX)]
    if es_codigo_kg(code):
        code = code[1:]
    return code


def gramaje_ancho(code):
    """(gramaje g/m², ancho en cm) que dice el código, o (None, None)."""
    match = NOMEN_RE.match(clean(code))
    if not match:
        return None, None
    grammage = float(match.group(3))
    width_cm = float(match.group(8))
    if not grammage or not 30 <= width_cm <= 350:
        return grammage or None, None
    return grammage, width_cm


def rendimiento(grammage, width_cm):
    """Metros por kilo de una tela de ``grammage`` g/m² y ``width_cm`` de ancho."""
    if not grammage or not width_cm:
        return 0.0
    return 1000.0 / (grammage * width_cm / 100.0)


def convertir(qty, from_unit, to_unit, m_por_kg=0.0, ancho_m=0.0):
    """Convierte entre 'm', 'yd', 'kg' y 'm2'. None si falta el dato que hace de
    puente (rendimiento para kilos, ancho para m²): nunca se inventa."""
    if qty is None:
        return None
    from_unit, to_unit = (from_unit or '').lower(), (to_unit or '').lower()
    if from_unit == to_unit:
        return qty
    if from_unit == 'm':
        meters = qty
    elif from_unit == 'yd':
        meters = qty * YARD_M
    elif from_unit == 'kg':
        if not m_por_kg:
            return None
        meters = qty * m_por_kg
    elif from_unit == 'm2':
        if not ancho_m:
            return None
        meters = qty / ancho_m
    else:
        return None
    if to_unit == 'm':
        return meters
    if to_unit == 'yd':
        return meters / YARD_M
    if to_unit == 'kg':
        return meters / m_por_kg if m_por_kg else None
    if to_unit == 'm2':
        return meters * ancho_m if ancho_m else None
    return None
