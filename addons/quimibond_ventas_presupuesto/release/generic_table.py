# -*- coding: utf-8 -*-
"""Lector genérico de releases «parte × fecha» (Excel o CSV) — Python puro.

Para los clientes sin lector propio (diseño v3, sección 2.10.4: Shawmut Silao
.csv, Tennessee, Zwisstex y los que mandan hoja propia). Reconoce dos formas:

- **Ancha**: una fila de encabezado con fechas en columnas (lunes, días o
  fechas de entrega) y una fila por parte con la cantidad de cada fecha.
- **Larga**: columnas «parte», «fecha» y «cantidad» (una fila por parte y
  fecha), como sale un reporte de SAP o un CSV de portal.

Lo que no se adivina va en el perfil de release: el encabezado de la columna
de la parte (si no, la primera columna de texto antes de las fechas), los de
OC y descripción, y el orden día/mes de las fechas de texto. Nada se inventa:
una celda que no es número no es cantidad (aviso), una cantidad 0 o vacía no
es semana.

Salida (la de los demás lectores)::

    {'parts': [{'customer_part', 'description', 'customer_po',
                'lines': [{'date', 'qty'}]}], 'warnings': [...], 'layout': 'ancha'|'larga'}
"""
import csv
import io
import re
from collections import OrderedDict
from datetime import date, datetime, timedelta

HEADER_ROWS = 25
LONG_PART = ('part', 'parte', 'item', 'material', 'numero de parte', 'no. parte', 'part number', 'part no',
             'codigo', 'código', 'sku')
LONG_DATE = ('date', 'fecha', 'delivery date', 'due date', 'ship date', 'fecha de entrega', 'semana', 'week')
LONG_QTY = ('qty', 'quantity', 'cantidad', 'cant', 'requerido', 'required', 'gross need', 'demand')


class GenericReleaseError(ValueError):
    """El archivo no tiene una tabla parte × fecha legible."""


def _norm(value):
    text = str(value if value is not None else '').strip().lower()
    text = (text.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o')
            .replace('ú', 'u'))
    return re.sub(r'\s+', ' ', text).rstrip(':').strip()


def to_date(value, order='dmy'):
    """Fecha de una celda (date, datetime, número de serie de Excel o texto); None si no es fecha."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and 40000 < value < 60000:  # serie de Excel (2009–2064)
        return date(1899, 12, 30) + timedelta(days=int(value))
    text = str(value or '').strip()
    if not text:
        return None
    m = re.fullmatch(r'(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T].*)?', text)
    if m:
        return _safe(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.fullmatch(r'(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})', text)
    if m:
        a, b, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        year += 2000 if year < 100 else 0
        day, month = (a, b) if order == 'dmy' else (b, a)
        return _safe(year, month, day)
    return None


def _safe(year, month, day):
    try:
        return date(year, month, day)
    except ValueError:
        return None


def number(value):
    """Cantidad de una celda; None si vacía; False si es texto."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace('\xa0', '').replace(' ', '').replace(',', '')
    if text in ('', '-'):
        return None
    try:
        return float(text)
    except ValueError:
        return False


def _cell(row, index):
    return row[index] if index is not None and index < len(row) else None


def _find_col(header, names, exclude=()):
    normalized = [_norm(c) for c in header]
    for wanted in names:
        wanted = _norm(wanted)
        for index, text in enumerate(normalized):
            if index not in exclude and text and (text == wanted or text.startswith(wanted + ' ')):
                return index
    return None


def _wide_header(rows, order):
    """(índice de fila, {columna: fecha}) de la primera fila con 2+ fechas."""
    for index, row in enumerate(rows[:HEADER_ROWS]):
        dates = {col: to_date(cell, order) for col, cell in enumerate(row)}
        dates = {col: d for col, d in dates.items() if d}
        if len(dates) >= 2:
            return index, dates
    return None, {}


def parse_rows(rows, part_header=None, po_header=None, desc_header=None, order='dmy'):
    """Filas (listas de celdas) → partes del release. Ver la docstring del módulo."""
    rows = [list(r) for r in rows if r is not None]
    warnings = []
    head_at, date_cols = _wide_header(rows, order)
    parts = OrderedDict()

    def add(part, description, po, day, qty, where):
        value = number(qty)
        if value is False:
            warnings.append("%s: «%s» no es una cantidad." % (where, qty))
            return
        if not value or value < 0:
            return
        entry = parts.setdefault(part, {'customer_part': part, 'description': description or None,
                                        'customer_po': po or None, 'lines': OrderedDict()})
        entry['lines'][day] = entry['lines'].get(day, 0.0) + value

    if head_at is not None:
        layout = 'ancha'
        header = rows[head_at]
        first_date = min(date_cols)
        exclude = set(date_cols)
        part_col = _find_col(header, [part_header], exclude) if part_header else None
        if part_col is None:
            part_col = _find_col(header[:first_date], LONG_PART, exclude)
        if part_col is None:
            # Primera columna antes de las fechas que trae texto en las filas de datos.
            for col in range(first_date):
                if any(isinstance(_cell(r, col), str) and _cell(r, col).strip()
                       for r in rows[head_at + 1:head_at + 20]):
                    part_col = col
                    break
        if part_col is None:
            raise GenericReleaseError("No se encontró la columna de la parte del cliente; indíquela en el perfil.")
        po_col = _find_col(header, [po_header], exclude) if po_header else None
        desc_col = _find_col(header, [desc_header], exclude) if desc_header else None
        for n, row in enumerate(rows[head_at + 1:], start=head_at + 2):
            part = str(_cell(row, part_col) or '').strip()
            if not part or _norm(part).startswith('total'):
                continue
            for col, day in date_cols.items():
                add(part, _cell(row, desc_col), _cell(row, po_col), day, _cell(row, col),
                    "Fila %d, %s" % (n, day.isoformat()))
    else:
        layout = 'larga'
        head_at = None
        for index, row in enumerate(rows[:HEADER_ROWS]):
            if _find_col(row, LONG_DATE) is not None and _find_col(row, LONG_QTY) is not None:
                head_at = index
                break
        if head_at is None:
            raise GenericReleaseError(
                "No se encontró una fila de fechas (tabla ancha) ni columnas de fecha y cantidad (tabla larga).")
        header = rows[head_at]
        date_col = _find_col(header, LONG_DATE)
        qty_col = _find_col(header, LONG_QTY, {date_col})
        part_col = _find_col(header, [part_header]) if part_header else None
        if part_col is None:
            part_col = _find_col(header, LONG_PART, {date_col, qty_col})
        if part_col is None:
            raise GenericReleaseError("No se encontró la columna de la parte del cliente; indíquela en el perfil.")
        po_col = _find_col(header, [po_header]) if po_header else None
        desc_col = _find_col(header, [desc_header]) if desc_header else None
        for n, row in enumerate(rows[head_at + 1:], start=head_at + 2):
            part = str(_cell(row, part_col) or '').strip()
            if not part:
                continue
            day = to_date(_cell(row, date_col), order)
            if not day:
                if _cell(row, date_col) not in (None, ''):
                    warnings.append("Fila %d: «%s» no es una fecha." % (n, _cell(row, date_col)))
                continue
            add(part, _cell(row, desc_col), _cell(row, po_col), day, _cell(row, qty_col), "Fila %d" % n)
    result = []
    for entry in parts.values():
        lines = [{'date': d, 'qty': q} for d, q in sorted(entry['lines'].items())]
        if lines:
            result.append(dict(entry, lines=lines))
    if not result:
        raise GenericReleaseError("La tabla no trae cantidades mayores a cero.")
    return {'parts': result, 'warnings': warnings, 'layout': layout}


def rows_from_file(content, filename=''):
    """Bytes del archivo → filas: .xlsx con openpyxl (primera hoja con datos), .csv/.txt con csv."""
    name = (filename or '').lower()
    if name.endswith('.xls'):
        raise GenericReleaseError("El archivo es .xls (Excel viejo): ábralo y guárdelo como .xlsx o .csv.")
    if name.endswith(('.csv', '.txt')) or not content[:2] == b'PK':
        text = None
        for encoding in ('utf-8-sig', 'latin-1'):
            try:
                text = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        try:
            dialect = csv.Sniffer().sniff(text[:4096], delimiters=',;\t|')
        except csv.Error:
            dialect = csv.excel
        return list(csv.reader(io.StringIO(text), dialect))
    try:
        import openpyxl
    except ImportError:  # pragma: no cover - en Odoo siempre está
        raise GenericReleaseError("openpyxl no está instalado.")
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:  # archivo dañado o no es Excel
        raise GenericReleaseError("No se pudo abrir el Excel: %s" % exc)
    for sheet in workbook.worksheets:
        rows = [list(r) for r in sheet.iter_rows(values_only=True)]
        if any(any(c not in (None, '') for c in r) for r in rows):
            return rows
    raise GenericReleaseError("El Excel no tiene datos.")


def parse_file(content, filename='', **options):
    return parse_rows(rows_from_file(content, filename), **options)
