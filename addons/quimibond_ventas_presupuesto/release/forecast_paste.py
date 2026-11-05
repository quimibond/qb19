# -*- coding: utf-8 -*-
"""«Pegar semanas desde Excel» (diseño v3, sección 2.3) — Python puro.

Jessica copia un bloque de la hoja del cliente en Excel (Ctrl+C) y lo pega en
Odoo. El portapapeles de Excel llega como texto: renglones separados por salto
de línea y celdas por tabulador. Este módulo lo convierte en renglones con sus
semanas, sin tocar Odoo:

- las primeras celdas de texto de cada renglón son identificadores (nuestro
  código de producto y, si viene, la parte del cliente);
- después vienen las semanas. Si el bloque trae el renglón de encabezado con
  los números de semana (1, 2, 3…), manda ese renglón; si no, la primera
  columna de cantidades es la semana ``first_week`` y las siguientes van en
  orden;
- al pasar de la semana 52 (o de un número mayor a uno menor en el
  encabezado) se brinca al año siguiente: cada semana es (desfase de año,
  número de semana);
- una celda vacía es «sin pronóstico» (``None``); un 0 es cero; un texto en
  una semana («por confirmar») no es cantidad y queda como aviso. Nada se
  inventa.
"""
import re

LABELS_SKIP = ('TOTAL', 'SUBTOTAL', 'SEMANA', 'FECHA', 'PRODUCTO', 'CLIENTE', 'PO', 'RENDIMIENTO',
               'CONVERSION', 'CONVERSIÓN')
MAX_WEEK = 53


def _cells(line):
    return [c.strip() for c in line.rstrip('\r').split('\t')]


def number(text):
    """Cantidad de una celda: '1,500' → 1500.0, '1500.5' → 1500.5, '' o '-' → None.
    Devuelve ``False`` si es texto (no es cantidad)."""
    text = (text or '').strip().replace('\xa0', '').replace(' ', '')
    if text in ('', '-', '—'):
        return None
    text = text.replace(',', '')
    try:
        return float(text)
    except ValueError:
        return False


def _week_header(cells):
    """{índice de columna: (desfase, semana)} si el renglón es un encabezado de semanas
    (4 o más enteros entre 1 y 53 en orden), si no None."""
    found = []
    for index, cell in enumerate(cells):
        value = number(cell)
        if value and value == int(value) and 1 <= value <= MAX_WEEK:
            found.append((index, int(value)))
    if len(found) < 4:
        return None
    # Un renglón de producto con cantidades chicas no es encabezado: las semanas van seguidas
    # (+1, o de 52/53 a 1) y antes de ellas solo hay vacío o una etiqueta («semana»).
    texts = [c for c in cells[:found[0][0]] if c]
    if any(not _is_label(t) for t in texts):
        return None
    mapping, offset, previous = {}, 0, None
    for index, week in found:
        if previous is not None:
            if week == 1 and previous >= 52:
                offset += 1
            elif week != previous + 1:
                return None
        mapping[index] = (offset, week)
        previous = week
    return mapping


def _leading_text(cells):
    """Cuántas celdas de texto (no número, no vacías) hay al principio del renglón."""
    count = 0
    for cell in cells:
        if not cell or number(cell) is not False:
            break
        count += 1
    return count


def _is_label(text):
    upper = text.upper().strip().rstrip(':')
    return any(upper == label or upper.startswith(label + ' ') for label in LABELS_SKIP)


def parse_paste(text, first_week=None):
    """Renglones del bloque pegado.

    Devuelve ``{'weeks': [(desfase, semana)…], 'rows': [{'ids': [...], 'qty': {(desfase, semana):
    cantidad o None}, 'line': n}], 'warnings': [...]}``. Sin encabezado de semanas hace falta
    ``first_week``."""
    lines = [_cells(line) for line in (text or '').split('\n')]
    lines = [cells for cells in lines if any(cells)]
    warnings = []
    header, header_at = None, None
    for position, cells in enumerate(lines[:6]):
        header = _week_header(cells)
        if header:
            header_at = position
            break
    data = [(n, cells) for n, cells in enumerate(lines, start=1) if n - 1 != header_at]
    if header:
        columns = header
    else:
        if not first_week or not 1 <= int(first_week) <= MAX_WEEK:
            return {'weeks': [], 'rows': [], 'warnings': [
                "El bloque no trae el renglón con los números de semana: indique la primera semana."]}
        start = max([_leading_text(cells) for _n, cells in data] or [0])
        width = max([len(cells) for _n, cells in data] or [0])
        columns, offset, week = {}, 0, int(first_week)
        for index in range(start, width):
            columns[index] = (offset, week)
            week += 1
            if week > 52:
                week, offset = 1, offset + 1
    first_data = min(columns) if columns else 0
    rows = []
    for n, cells in data:
        ids = [c for c in cells[:first_data] if c and number(c) is False]
        if not ids or _is_label(ids[0]):
            continue
        qty = {}
        for index, key in columns.items():
            cell = cells[index] if index < len(cells) else ''
            value = number(cell)
            if value is False:
                warnings.append("Renglón %d (%s), semana %d: «%s» no es una cantidad." % (
                    n, ids[0], key[1], cell))
                continue
            if value is not None and value < 0:
                warnings.append("Renglón %d (%s), semana %d: cantidad negativa (%s), no se toma." % (
                    n, ids[0], key[1], cell))
                continue
            qty[key] = value
        rows.append({'ids': ids, 'qty': qty, 'line': n})
    weeks = [columns[index] for index in sorted(columns)]
    if not rows:
        warnings.append("No se encontró ningún renglón con código de producto.")
    return {'weeks': weeks, 'rows': rows, 'warnings': warnings}


def norm_code(text):
    """Código como se compara contra ``default_code``: mayúsculas, sin espacios ni punto final."""
    return re.sub(r'\s+', '', (text or '').upper()).rstrip('.')
