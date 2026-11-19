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
    """Texto del PDF con su acomodo (columnas alineadas con espacios). Con pypdf ≥ 3.17 se usa su modo
    «layout»; con versiones anteriores (o PyPDF2, como en Odoo.sh) se arma con las posiciones del texto
    (``stream_layout_text``)."""
    reader = reader_cls(io.BytesIO(content))
    pages = []
    for page in reader.pages:
        try:
            pages.append(page.extract_text(extraction_mode='layout') or '')
        except TypeError:  # pypdf < 3.17 o PyPDF2: sin modo layout
            pages.append(stream_layout_text(_page_stream(page)))
    return "\n".join(pages)


def _page_stream(page):
    """Bytes del contenido de una página con pypdf o con PyPDF2."""
    getter = getattr(page, 'get_contents', None) or getattr(page, 'getContents')
    contents = getter()
    if contents is None:
        return b''
    data = getattr(contents, 'get_data', None) or getattr(contents, 'getData')
    return data()


_DELIMS = b'()<>[]{}/%'
_WS = b' \t\r\n\x0c\x00'
_ESCAPES = {b'n': b'\n', b'r': b'\r', b't': b'\t', b'b': b'\b', b'f': b'\f'}


def _pdf_tokens(data):
    """Tokens de un content stream: ('s', bytes) cadenas, ('n', float) números, ('o', str) operadores y
    nombres, '[' y ']'. Suficiente para el texto de un PDF de Excel (fuentes WinAnsi, celdas con Tm)."""
    i, n = 0, len(data)
    while i < n:
        c = data[i:i + 1]
        if c in _WS:
            i += 1
        elif c == b'%':
            while i < n and data[i:i + 1] not in b'\r\n':
                i += 1
        elif c == b'(':
            depth, i, buf = 1, i + 1, bytearray()
            while i < n and depth:
                ch = data[i:i + 1]
                if ch == b'\\':
                    nxt = data[i + 1:i + 2]
                    if nxt in _ESCAPES:
                        buf += _ESCAPES[nxt]
                        i += 2
                    elif nxt.isdigit():
                        j = i + 1
                        while j < min(i + 4, n) and data[j:j + 1].isdigit():
                            j += 1
                        buf.append(int(data[i + 1:j], 8) & 0xFF)
                        i = j
                    elif nxt in b'\r\n':
                        i += 2
                    else:
                        buf += nxt
                        i += 2
                    continue
                if ch == b'(':
                    depth += 1
                elif ch == b')':
                    depth -= 1
                    if not depth:
                        i += 1
                        break
                buf += ch
                i += 1
            yield ('s', bytes(buf))
        elif c == b'<' and data[i + 1:i + 2] != b'<':
            j = data.find(b'>', i)
            j = n if j < 0 else j
            hexs = re.sub(rb'\s', b'', data[i + 1:j])
            if len(hexs) % 2:
                hexs += b'0'
            try:
                yield ('s', bytes.fromhex(hexs.decode('ascii')))
            except ValueError:
                pass
            i = j + 1
        elif c in (b'[', b']'):
            yield (c.decode(), None)
            i += 1
        elif c in (b'<', b'>'):  # << >> de diccionarios en línea (BDC): se ignoran
            i += 2 if data[i + 1:i + 2] == c else 1
        else:
            j = i + 1 if c in (b'/', b'{', b'}') else i
            while j < n and data[j:j + 1] not in _WS and data[j:j + 1] not in _DELIMS:
                j += 1
            word = data[i:j].decode('latin-1')
            i = max(j, i + 1)
            try:
                yield ('n', float(word))
            except ValueError:
                yield ('o', word)


def _mul(a, b):
    """Producto de matrices PDF [a b c d e f]."""
    return [a[0] * b[0] + a[1] * b[2], a[0] * b[1] + a[1] * b[3],
            a[2] * b[0] + a[3] * b[2], a[2] * b[1] + a[3] * b[3],
            a[4] * b[0] + a[5] * b[2] + b[4], a[4] * b[1] + a[5] * b[3] + b[5]]


def _text_runs(data):
    """(x, y, tamaño, texto) de cada texto que se muestra en el content stream."""
    ident = [1.0, 0.0, 0.0, 1.0, 0.0, 0.0]
    ctm, stack = list(ident), []
    tm = lm = list(ident)
    size, leading = 1.0, 0.0
    operands, array, runs = [], None, []

    def show(chunks):
        text = ''.join(chunks).strip()
        if text:
            m = _mul(tm, ctm)
            runs.append((m[4], m[5], abs(size * m[3]) or abs(size), text))

    def dec(raw):
        return raw.decode('cp1252', errors='replace')

    for kind, value in _pdf_tokens(data):
        if kind == '[':
            array = []
            continue
        if kind == ']':
            operands.append(('a', array or []))
            array = None
            continue
        if array is not None:
            array.append((kind, value))
            continue
        if kind != 'o' or value.startswith('/'):
            operands.append((kind, value))
            continue
        nums = [v for k, v in operands if k == 'n']
        op = value
        if op == 'q':
            stack.append(list(ctm))
        elif op == 'Q':
            ctm = stack.pop() if stack else list(ident)
        elif op == 'cm' and len(nums) >= 6:
            ctm = _mul(nums[-6:], ctm)
        elif op == 'BT':
            tm = lm = list(ident)
        elif op == 'Tf' and nums:
            size = nums[-1]
        elif op == 'TL' and nums:
            leading = nums[-1]
        elif op == 'Tm' and len(nums) >= 6:
            tm = lm = nums[-6:]
        elif op in ('Td', 'TD') and len(nums) >= 2:
            if op == 'TD':
                leading = -nums[-1]
            tm = lm = _mul([1, 0, 0, 1, nums[-2], nums[-1]], lm)
        elif op == 'T*':
            tm = lm = _mul([1, 0, 0, 1, 0, -leading], lm)
        elif op in ('Tj', "'", '"'):
            if op != 'Tj':
                tm = lm = _mul([1, 0, 0, 1, 0, -leading], lm)
            show([dec(v) for k, v in operands if k == 's'][-1:])
        elif op == 'TJ':
            chunks = []
            for k, v in next((v for k, v in reversed(operands) if k == 'a'), []):
                if k == 's':
                    chunks.append(dec(v))
                elif k == 'n' and v < -250:  # un hueco grande dentro del arreglo es un espacio
                    chunks.append(' ')
            show(chunks)
        operands = []
    return runs


def stream_layout_text(data):
    """Texto con acomodo armado con las posiciones del content stream: un renglón por fila de la hoja, cada
    texto en la columna de su x. La escala es medio tamaño de letra por carácter (ancho medio de Arial), así
    que las celdas quedan donde están en la hoja y separadas por espacios.

    Excel centra en la vertical las celdas de una fila, así que sus textos no comparten la misma y: se juntan
    los que están a menos de dos tercios de letra uno del otro. Si dos textos de una fila caen en la misma columna
    (descripción en dos renglones), el que está más lejos de la fila sale en su propio renglón, arriba o
    abajo, como lo hace pypdf."""
    runs = _text_runs(data)
    if not runs:
        return ''
    sizes = sorted(r[2] for r in runs)
    unit = max(sizes[len(sizes) // 2] * 0.5, 0.5)  # la letra más común (la de las celdas), no la más chica
    groups = []
    for run in sorted(runs, key=lambda r: (-r[1], r[0])):
        if groups and groups[-1][-1][1] - run[1] <= max(run[2], groups[-1][-1][2]) * 0.65:
            groups[-1].append(run)
        else:
            groups.append([run])

    def span(run):
        start = run[0] / unit
        return start, start + len(run[3]) * run[2] * 0.5 / unit

    out = []
    for group in groups:
        ys = [round(r[1], 1) for r in group]
        mode = max(set(ys), key=lambda y: (ys.count(y), -abs(y - group[0][1])))
        anchor = next(r[1] for r in group if round(r[1], 1) == mode)
        main, above, below = [], [], []
        # A igual distancia gana el de arriba: el primer renglón de una celda de dos va con la fila.
        for run in sorted(group, key=lambda r: (round(abs(r[1] - anchor), 2), -r[1])):
            s0, e0 = span(run)
            if any(s0 < e1 and e0 > s1 for s1, e1 in (span(m) for m in main)):
                (above if run[1] > anchor else below).append(run)
            else:
                main.append(run)
        for cells in (above, main, below):
            if not cells:
                continue
            line = ''
            for run in sorted(cells):
                col = int(round(run[0] / unit))
                if line and col < len(line) + 2:
                    col = len(line) + 2
                line = line.ljust(col) + run[3]
            out.append(line)
    return "\n".join(out)


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
