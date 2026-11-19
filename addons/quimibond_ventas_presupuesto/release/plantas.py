# -*- coding: utf-8 -*-
"""Qué planta de un cliente es (19.0.3.30.0, Jose 19-nov-2026).

Python puro. La planta se escribe distinto en cada lugar: el perfil de release dice «SALTILLO LAMINATION
LEON», «FXI CD JUAREZ» o «Silao»; «Cómo pide cada cliente» dice «LEÓN», «CIUDAD JUAREZ» o «SILAO»; las
condiciones de entrega pueden tener solo la dirección de entrega. Se comparan por palabras, sin acentos ni
mayúsculas, sin las palabras del nombre del cliente (salvo que no quede otra) y con «cd» = «ciudad». Gana la
que comparte más palabras; si empatan, ninguna.
"""
import re
import unicodedata

SINONIMOS = {'cd': 'ciudad', 'cdad': 'ciudad', 'gto': 'guanajuato', 'tn': 'tennessee'}


def palabras(text):
    text = unicodedata.normalize('NFKD', str(text or '')).encode('ascii', 'ignore').decode().lower()
    return {SINONIMOS.get(w, w) for w in re.split(r'[^a-z0-9]+', text) if w}


def _nucleo(text, cliente):
    words = palabras(text)
    return (words - cliente) or words


def elegir(planta, opciones, cliente=''):
    """Índice de la opción que corresponde a ``planta`` (None si ninguna o empate)."""
    if not planta:
        return None
    nombre = palabras(cliente)
    target = _nucleo(planta, nombre)
    exacto = str(planta).strip().lower()
    scores = []
    for i, opcion in enumerate(opciones):
        if not opcion:
            continue
        if str(opcion).strip().lower() == exacto:
            scores.append((1000, i))
            continue
        common = len(target & _nucleo(opcion, nombre))
        if common:
            scores.append((common, i))
    if not scores:
        return None
    best = max(s for s, _i in scores)
    winners = [i for s, i in scores if s == best]
    return winners[0] if len(winners) == 1 else None
