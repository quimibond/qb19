# -*- coding: utf-8 -*-
"""Lector de releases en PDF con tabla «parte × fecha» (19.0.3.27.0, COPO) — Python puro.

COPO manda su release como un PDF hecho de Excel: una fila por parte (CTMX Part Number), descripción, última
factura considerada, OC, «Llegadas CTM» y una columna por semana con la fecha. El texto plano del PDF pierde las
semanas vacías (las cantidades se recorren), así que se lee con el **acomodo** del texto (``extraction_mode=
'layout'`` de pypdf): cada celda queda en la columna cuyo encabezado le queda más cerca.

- Encabezado: la primera línea con 2 o más fechas (16-Nov-26, 16/11/26…). Lo que está justo arriba o abajo de un
  encabezado se le junta (CTMX / Part Number).
- Una línea sin parte ni cantidades es la continuación de la fila de arriba (descripción en dos renglones).
- La tabla resultante pasa por el lector genérico (``generic_table.parse_rows``): mismos encabezados del perfil.
- Además se toman, si están, «Release No.», la fecha del release, la última factura considerada y lo recibido
  (Llegadas / CUM) de cada parte.
"""
import io
import re

try:
    from . import generic_table
except ImportError:  # pruebas puras (tests_puros): el módulo se carga suelto, sin paquete
    import importlib.util
    import os
    _spec = importlib.util.spec_from_file_location(
        'qb_release_generic_table', os.path.join(os.path.dirname(__file__), 'generic_table.py'))
    generic_table = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(generic_table)

_INVOICE_HEADERS = ('last invoice', 'ultima factura', 'última factura', 'packing slip', 'last shipment')
_RECEIVED_HEADERS = ('llegadas', 'cum received', 'received', 'recibido', 'cum')
_RELEASE_NO = re.compile(r'Release\s*(?:No\.?|N[°º]|#|Number)\s*:?\s+(\S+)', re.IGNORECASE)
_RELEASE_DATE = re.compile(r'(?:Date\s+Release|Release\s+Date|Fecha(?:\s+del)?\s+release)\s*:?\s+(\S+)',
                           re.IGNORECASE)


class PdfTableError(ValueError):
    """El PDF no trae una tabla parte × fecha legible."""


def pdf_layout_text(content, reader_cls):
    """Texto del PDF con su acomodo (columnas alineadas con espacios)."""
    reader = reader_cls(io.BytesIO(content))
    pages = []
    for page in reader.pages:
        try:
            pages.append(page.extract_text(extraction_mode='layout') or '')
        except TypeError as exc:  # pypdf < 3.17 no tiene el modo layout
            raise PdfTableError("La versión de pypdf del servidor no lee el acomodo del PDF.") from exc
    return "\n".join(pages)


def _tokens(line):
    """Celdas de una línea: textos separados por 2 o más espacios, con su posición (inicio, fin)."""
    out = []
    for m in re.finditer(r'\S+(?: \S+)*', line):
        out.append((m.start(), m.end(), m.group()))
    return out


def _is_date(text, order):
    return generic_table.to_date(text, order) is not None


def text_rows(text, order='dmy'):
    """Texto con acomodo → (filas de celdas, extras {'release_ref', 'release_date'})."""
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    head_at = None
    for index, line in enumerate(lines):
        if sum(1 for _s, _e, t in _tokens(line) if _is_date(t, order)) >= 2:
            head_at = index
            break
    if head_at is None:
        raise PdfTableError("El PDF no trae una fila de fechas (tabla parte × fecha).")
    columns = [[s, e, t] for s, e, t in _tokens(lines[head_at])]

    def nearest(start, end):
        center = (start + end) / 2.0
        overlap = [i for i, (s, e, _t) in enumerate(columns) if start < e and end > s]
        if overlap:
            return overlap[0]
        return min(range(len(columns)), key=lambda i: abs((columns[i][0] + columns[i][1]) / 2.0 - center))

    # Encabezados en dos renglones (CTMX / Part Number): lo que está pegado arriba o abajo y antes de las fechas.
    first_date = min(i for i, (_s, _e, t) in enumerate(columns) if _is_date(t, order))
    for neighbour in (head_at - 1, head_at + 1):
        if not 0 <= neighbour < len(lines):
            continue
        toks = _tokens(lines[neighbour])
        if not toks or any(_is_date(t, order) or generic_table.number(t) not in (False, None) for _s, _e, t in toks):
            continue
        for s, e, t in toks:
            col = nearest(s, e)
            if col < first_date:
                columns[col][2] = ("%s %s" % (t, columns[col][2]) if neighbour < head_at
                                   else "%s %s" % (columns[col][2], t))
    rows = [[t for _s, _e, t in columns]]
    date_cols = {i for i, (_s, _e, t) in enumerate(columns) if _is_date(t, order)}
    skip = head_at + 1 if head_at + 1 < len(lines) and not any(
        generic_table.number(t) not in (False, None) for _s, _e, t in _tokens(lines[head_at + 1])) else head_at
    for line in lines[skip + 1:]:
        toks = _tokens(line)
        if any(sum(1 for cs, ce, _t in columns if s < ce and e > cs) > 1 for s, e, _t in toks):
            break  # un texto que cruza varias columnas: notas al pie, se acabó la tabla
        cells = [''] * len(columns)
        for s, e, t in toks:
            col = nearest(s, e)
            cells[col] = ("%s %s" % (cells[col], t)).strip()
        has_qty = any(generic_table.number(cells[i]) for i in date_cols)
        has_key = any(cells[i] for i in range(first_date) if i not in date_cols and generic_table.number(cells[i]))
        if not has_qty and len(rows) > 1 and not has_key and any(cells):
            # Continuación de la fila de arriba (descripción en dos renglones).
            prev = rows[-1]
            for i, cell in enumerate(cells):
                if cell and i not in date_cols:
                    prev[i] = ("%s %s" % (prev[i], cell)).strip()
            continue
        if not has_qty:
            continue  # notas al pie, totales sin cantidades
        rows.append(cells)
    extras = {}
    m = _RELEASE_NO.search(text)
    if m:
        extras['release_ref'] = m.group(1)
    m = _RELEASE_DATE.search(text)
    if m:
        extras['release_date'] = generic_table.to_date(m.group(1), order)
    return rows, extras


def _col(header, names):
    for i, cell in enumerate(header):
        low = generic_table._norm(cell)
        if any(low.startswith(generic_table._norm(n)) or (' ' + generic_table._norm(n)) in low for n in names):
            return i
    return None


def parse_text(text, order='dmy', **options):
    """Texto con acomodo → partes del release (salida de ``generic_table.parse_rows``) más, por parte,
    ``last_invoice`` y ``cum_received`` si el PDF los trae, y ``release_ref``/``release_date``."""
    rows, extras = text_rows(text, order)
    header = rows[0]
    # Los encabezados del PDF pueden juntar dos renglones (CTMX Part Number): se buscan por contenido.
    for key, col_key, defaults in (('part_header', 'part_col', ('part number', 'parte', 'part no', 'item')),
                                   ('po_header', 'po_col', ()), ('desc_header', 'desc_col', ())):
        wanted = options.pop(key, None)
        if options.get(col_key) is None:
            found = _col(header, [wanted] if wanted else defaults) if (wanted or defaults) else None
            if found is not None:
                options[col_key] = found
    data = generic_table.parse_rows(rows, order=order, **options)
    inv_col, rec_col = _col(header, _INVOICE_HEADERS), _col(header, _RECEIVED_HEADERS)
    by_part = {}
    for row in rows[1:]:
        for cell in row:
            by_part.setdefault(cell, row)
    for part in data['parts']:
        row = by_part.get(part['customer_part'])
        if not row:
            continue
        if inv_col is not None and row[inv_col]:
            part['last_invoice'] = row[inv_col]
        received = generic_table.number(row[rec_col]) if rec_col is not None else None
        if received:
            part['cum_received'] = received
    data.update(extras)
    return data


def parse_pdf(content, reader_cls, order='dmy', **options):
    return parse_text(pdf_layout_text(content, reader_cls), order=order, **options)
