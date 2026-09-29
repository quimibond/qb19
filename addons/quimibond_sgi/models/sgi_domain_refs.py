# -*- coding: utf-8 -*-
"""Dominios portables (H-007, entrega 3): los IDs fijos de un dominio se
vuelven marcas ``%(r1)s`` con una referencia que se resuelve en la base que
carga.

Este módulo es Python puro (sin Odoo): lo usan ``export_payload`` y
``load_payload`` (sgi_load.py / sgi_export.py) y el generador del mapa
(``quimibond_sgi_mapa/tools/generar_mapa.py``), que corre fuera de Odoo.

- ``templatize(texto, decidir)``: recorre las hojas ``(campo, operador,
  valor)`` del dominio y, por cada entero que va como valor (solo o dentro de
  una lista), llama ``decidir(campo, valor)``; si devuelve un nombre, el
  entero se sustituye por ``%(nombre)s`` en el MISMO texto (se conserva el
  formato original, así que resolverlo en la base de origen da exactamente el
  texto que había). Los booleanos, los negativos y los números de campos que
  no son relaciones los decide quien llama.
- ``fill(plantilla, resueltos)``: sustituye cada marca por su id; devuelve el
  texto y la lista de marcas que no se resolvieron.

Una referencia (el valor de ``refs[nombre]``) es un diccionario:

- ``{"model": "res.company", "company": true}``: la empresa de la carga;
- ``{"model": "...", "xmlid": "modulo.nombre"}``;
- ``{"model": "...", "key": {"campo": valor, "otro.campo": valor}}``: un solo
  registro con esos valores (en la empresa de la carga o compartido);
- ``{"model": "...", "id": 13, "name": "Cliente extranjero"}``: último
  recurso, cuando no hay llave única. Solo se resuelve si en la base existe
  ese id Y se llama igual (una copia de producción); en cualquier otra base
  queda sin resolver y se avisa, nunca mide otra cosa en silencio.
"""
import ast
import re

PLACEHOLDER = re.compile(r"%\((\w+)\)s")
# Dominio que no encuentra nada: lo que queda cuando una referencia no existe
# en la base que carga (se avisa en el reporte; mide cero, no otra cosa).
INERT_DOMAIN = "[('id', '=', 0)]"


def _is_id(node):
    return isinstance(node, ast.Constant) and type(node.value) is int and node.value > 0


def _offsets(source):
    """Inicio (en bytes UTF-8) de cada línea: ast da columnas en bytes."""
    starts, total = [0], 0
    for line in source.encode('utf-8').split(b'\n'):
        total += len(line) + 1
        starts.append(total)
    return starts


def leaf_ids(text):
    """[(inicio, fin, campo, valor)] de cada id que va como valor de una hoja
    del dominio; inicio/fin en bytes UTF-8 del texto. Un texto que no se
    puede leer como dominio da []."""
    text = text or ''
    body = text.lstrip()
    shift = len(text[:len(text) - len(body)].encode('utf-8'))
    try:
        tree = ast.parse(body, mode='eval').body
    except (SyntaxError, ValueError):
        return []
    if not isinstance(tree, (ast.List, ast.Tuple)):
        return []
    starts = _offsets(body)
    found = []
    for leaf in tree.elts:
        if not isinstance(leaf, (ast.List, ast.Tuple)) or len(leaf.elts) != 3:
            continue
        name = leaf.elts[0]
        if not (isinstance(name, ast.Constant) and isinstance(name.value, str)):
            continue
        value = leaf.elts[2]
        if _is_id(value):
            nodes = [value]
        elif isinstance(value, (ast.List, ast.Tuple)):
            nodes = [e for e in value.elts if _is_id(e)]
        else:
            nodes = []
        for node in nodes:
            begin = shift + starts[node.lineno - 1] + node.col_offset
            end = shift + starts[node.end_lineno - 1] + node.end_col_offset
            found.append((begin, end, name.value, node.value))
    return found


def templatize(text, decide):
    """(plantilla, [nombres usados]). ``decide(campo, valor)`` devuelve el
    nombre de la marca o None para dejar el número como está."""
    spans = leaf_ids(text)
    if not spans:
        return text, []
    raw = text.encode('utf-8')
    out, last, used = [], 0, []
    for begin, end, field, value in spans:
        name = decide(field, value)
        if not name:
            continue
        out.append(raw[last:begin])
        out.append(('%%(%s)s' % name).encode('utf-8'))
        last = end
        if name not in used:
            used.append(name)
    out.append(raw[last:])
    return b''.join(out).decode('utf-8'), used


def names(template):
    """Marcas que usa una plantilla, en orden y sin repetir."""
    seen = []
    for name in PLACEHOLDER.findall(template or ''):
        if name not in seen:
            seen.append(name)
    return seen


def fill(template, resolved):
    """(texto, [marcas sin resolver]). ``resolved``: {marca: id}."""
    missing = []

    def repl(match):
        value = resolved.get(match.group(1))
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        if match.group(1) not in missing:
            missing.append(match.group(1))
        return match.group(0)
    return PLACEHOLDER.sub(repl, template or ''), missing
