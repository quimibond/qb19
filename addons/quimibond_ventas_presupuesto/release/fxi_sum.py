# -*- coding: utf-8 -*-
"""Lector del release de FXI: Excel «SUM Quimibond MM.DD.YY.xlsx».

Es un reporte SAP («Dynamic List Display / Summary Report») volcado a una
hoja. Por cada parte:

    fila de encabezado: Raw Material No. | Raw Material Desc | Item No. | …
                        Vendor Authorization. | Raw Accum Rcvd | Plant |
                        Release # | Release Date | Agreement # | UOM | …
    fila de valores
    fila «Date»        con 52 fechas (lunes)
    fila «Gross need»  cantidad semanal
    fila «V Cum»       acumulado

Las fechas son de ENTREGA en FXI («Delivery Releases»). Python puro: recibe
filas (listas de celdas) para no depender de openpyxl aquí; ``parse_xlsx``
abre el archivo si openpyxl está instalado (lo está en Odoo).

Salida:
    {'vendor': '105925', 'parts': [{'customer_part', 'description', 'item',
      'vendor_auth_qty', 'raw_accum_rcvd', 'plant', 'release_ref',
      'release_date', 'agreement', 'uom',
      'lines': [{'date', 'gross_need', 'cum'}]}]}
"""
import io
import re
from datetime import date, datetime


class FxiReleaseError(ValueError):
    """El archivo no es un release SUM de FXI legible."""


# Encabezado normalizado → clave de salida.
_COLUMNS = {
    'raw material no': 'customer_part',
    'raw material desc': 'description',
    'item no': 'item',
    'vendor authorization': 'vendor_auth_qty',
    'raw accum rcvd': 'raw_accum_rcvd',
    'plant': 'plant',
    'release #': 'release_ref',
    'release date': 'release_date',
    'agreement #': 'agreement',
    'uom': 'uom',
}
_REQUIRED = ('customer_part', 'release_ref', 'release_date', 'uom')
_NUMERIC = ('vendor_auth_qty', 'raw_accum_rcvd')


def _norm(value):
    return re.sub(r'\s+', ' ', str(value or '').strip().lower()).rstrip('.').strip()


def _number(value):
    if value in (None, ''):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(',', '').replace('"', '').strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        raise FxiReleaseError("Cantidad ilegible: %r" % value)


def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or '').strip()
    if re.fullmatch(r'\d{8}', text):  # AAAAMMDD
        return date(int(text[:4]), int(text[4:6]), int(text[6:]))
    m = re.fullmatch(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', text)
    if m:
        year = int(m.group(3))
        if year < 100:
            year += 2000
        return date(year, int(m.group(1)), int(m.group(2)))
    raise FxiReleaseError("Fecha ilegible: %r" % value)


def _label(row):
    """Primera celda de texto de la fila (la etiqueta «Date», «Gross need»…)."""
    for cell in row:
        if cell not in (None, ''):
            return _norm(cell)
    return ''


def parse_rows(rows):
    """Filas de la hoja (listas de celdas) → diccionario del release."""
    rows = [list(r) for r in rows]
    vendor = None
    for row in rows:
        texts = [str(c) for c in row if c not in (None, '')]
        if texts and _norm(texts[0]).startswith('vendor') and len(texts) > 1 \
                and 'address' not in _norm(texts[0]):
            vendor = str(texts[1]).strip()
            break
    parts = []
    i = 0
    while i < len(rows):
        row = rows[i]
        normalized = [_norm(c) for c in row]
        if 'raw material no' not in normalized:
            i += 1
            continue
        columns = {idx: _COLUMNS[name] for idx, name in enumerate(normalized)
                   if name in _COLUMNS}
        if i + 1 >= len(rows):
            raise FxiReleaseError("Encabezado sin fila de valores al final del archivo.")
        values = rows[i + 1]
        part = {'lines': []}
        for idx, key in columns.items():
            value = values[idx] if idx < len(values) else None
            if key in _NUMERIC:
                part[key] = _number(value)
            elif key == 'release_date':
                part[key] = _to_date(value) if value not in (None, '') else None
            else:
                part[key] = str(value).strip() if value not in (None, '') else ''
        for key in _REQUIRED:
            if not part.get(key):
                raise FxiReleaseError("La parte %s no trae «%s»." % (
                    part.get('customer_part') or '?', key))
        # Filas Date / Gross need / V Cum que siguen.
        blocks = {}
        j = i + 2
        while j < len(rows) and len(blocks) < 3:
            label = _label(rows[j])
            if 'raw material no' in [_norm(c) for c in rows[j]]:
                break
            if label in ('date', 'gross need', 'v cum'):
                blocks[label] = rows[j]
            j += 1
        if 'date' not in blocks or 'gross need' not in blocks:
            raise FxiReleaseError("La parte %s no trae las filas «Date» y «Gross need»."
                                  % part['customer_part'])
        date_row, need_row = blocks['date'], blocks['gross need']
        cum_row = blocks.get('v cum', [])
        start = [_norm(c) for c in date_row].index('date') + 1
        for col in range(start, len(date_row)):
            cell = date_row[col]
            if cell in (None, ''):
                continue
            part['lines'].append({
                'date': _to_date(cell),
                'gross_need': _number(need_row[col] if col < len(need_row) else 0),
                'cum': _number(cum_row[col] if col < len(cum_row) else 0),
            })
        if not part['lines']:
            raise FxiReleaseError("La parte %s no trae semanas." % part['customer_part'])
        dates = [line['date'] for line in part['lines']]
        if len(set(dates)) != len(dates):
            raise FxiReleaseError("La parte %s repite semanas." % part['customer_part'])
        parts.append(part)
        i = j
    if not parts:
        raise FxiReleaseError("No es un release SUM de FXI: no hay «Raw Material No.».")
    return {'vendor': vendor, 'parts': parts}


def parse_xlsx(content):
    """Bytes del .xlsx → diccionario del release (primera hoja)."""
    try:
        import openpyxl
    except ImportError:  # pragma: no cover - en Odoo siempre está
        raise FxiReleaseError("openpyxl no está instalado.")
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True,
                                          data_only=True)
    except Exception as exc:  # archivo dañado o no es Excel
        raise FxiReleaseError("No se pudo abrir el Excel: %s" % exc)
    sheet = workbook.worksheets[0]
    return parse_rows(sheet.iter_rows(values_only=True))
