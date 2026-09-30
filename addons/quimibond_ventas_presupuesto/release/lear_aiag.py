# -*- coding: utf-8 -*-
"""Lector del release de Lear: texto AIAG «SUPPLIER SCHEDULE / MATERIAL RELEASE».

Python puro. Entra el texto (cuerpo del .eml adjunto «QUIMIBOND.eml», que es
la impresión de la transacción EDI) y sale un diccionario:

    {'supplier': '6PIN0010', 'ship_to': '5500',
     'parts': [{'release_ref', 'release_date', 'customer_po', 'buyer',
                'customer_part', 'description', 'uom', 'in_transit_qty',
                'last_receipt_date', 'last_receipt_qty', 'cum_received',
                'packing_slip', 'fab_auth_qty', 'fab_auth_date',
                'raw_auth_qty', 'raw_auth_date', 'prior_req_qty',
                'prior_cum_req_qty',
                'lines': [{'date', 'qty_type', 'req_qty', 'cum_req_qty',
                           'net_req_qty'}]}]}

Un release puede traer varias partes (cada una con su encabezado) y el
encabezado se repite en cada página (salto ``\\f``): una parte se identifica
por Release ID + Item Number, y las páginas repetidas se juntan. Cualquier
dato que falte o no se entienda levanta ``LearReleaseError`` con el motivo:
el release queda «por revisar», nunca se aplica a medias.
"""
import re
from datetime import date


class LearReleaseError(ValueError):
    """El texto no es un release de Lear legible."""


_NUM = r'-?[\d,]+(?:\.\d+)?'
# Un valor de texto: sin espacios y que no sea la siguiente etiqueta («Buyer:»).
_VAL = r'(?![A-Za-z/-]+:)([^\s]+)'
_DATE = r'\d{2}/\d{2}/\d{2,4}'

_HEADER_FIELDS = {
    'supplier': re.compile(r'Supplier:[ \t]*' + _VAL),
    'ship_to': re.compile(r'Ship-To:[ \t]*' + _VAL),
}
_PART_FIELDS = {
    'release_ref': re.compile(r'Release ID:[ \t]*' + _VAL),
    'release_date': re.compile(r'Release Date:\s*(%s)' % _DATE),
    'customer_po': re.compile(r'Purchase Order:[ \t]*' + _VAL),
    'buyer': re.compile(r'Buyer:[ \t]*' + _VAL),
    'customer_part': re.compile(r'Item Number:[ \t]*' + _VAL),
    'uom': re.compile(r'\bUM:[ \t]*' + _VAL),
    'in_transit_qty': re.compile(r'In Transit Qty:\s*(%s)' % _NUM),
    'last_receipt_date': re.compile(r'Receipt Date:\s*(%s)' % _DATE),
    'last_receipt_qty': re.compile(r'Receipt Qty:\s*(%s)' % _NUM),
    'cum_received': re.compile(r'Cum Received:\s*(%s)' % _NUM),
    'packing_slip': re.compile(r'Packing Slip/Shipper:[ \t]*' + _VAL),
    'fab_auth': re.compile(r'Fab Authorization Cum Qty:\s*(%s)\s*Thru:\s*(%s)' % (_NUM, _DATE)),
    'raw_auth': re.compile(r'Raw Authorization Cum Qty:\s*(%s)\s*Thru:\s*(%s)' % (_NUM, _DATE)),
}
# Fila semanal: [Weekly]  fecha  [hora]  [referencia]  Q  Req  Cum Req  Net Req
_LINE_RE = re.compile(
    r'^\s*(?:\w+\s+)?(%s)\s+(?:\d{1,2}:\d{2}\s+)?(?:\S+\s+)?([A-Z])\s+(%s)\s+(%s)\s+(%s)\s*$'
    % (_DATE, _NUM, _NUM, _NUM))
_PRIOR_RE = re.compile(r'^\s*Prior\s+(%s)\s+(%s)' % (_NUM, _NUM))
# Descripción: el texto a la izquierda de «Receipt Date:» / «Receipt Qty:»
# en las dos líneas que siguen a «Item Number».
_DESC_RE = re.compile(r'^\s+(.+?)\s{2,}Receipt (?:Date|Qty):')


def _num(text):
    return float(text.replace(',', ''))


def _date(text):
    month, day, year = text.split('/')
    year = int(year)
    if year < 100:
        year += 2000
    return date(year, int(month), int(day))


def _pages(text):
    """Bloques por parte: cada «Release ID:» abre uno nuevo."""
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    blocks, current = [], []
    for raw in text.split('\n'):
        for chunk in raw.split('\f'):
            if 'Release ID:' in chunk and current:
                blocks.append('\n'.join(current))
                current = []
            current.append(chunk)
    if current:
        blocks.append('\n'.join(current))
    return [b for b in blocks if 'Release ID:' in b]


def _parse_block(block):
    part = {'lines': []}
    for key, regex in _PART_FIELDS.items():
        m = regex.search(block)
        if not m:
            continue
        if key in ('fab_auth', 'raw_auth'):
            part['%s_qty' % key] = _num(m.group(1))
            part['%s_date' % key] = _date(m.group(2))
        elif key in ('release_date', 'last_receipt_date'):
            part[key] = _date(m.group(1))
        elif key in ('in_transit_qty', 'last_receipt_qty', 'cum_received'):
            part[key] = _num(m.group(1))
        else:
            part[key] = m.group(1)
    lines = block.split('\n')
    desc = []
    for i, line in enumerate(lines):
        if 'Item Number:' in line:
            for follow in lines[i + 1:i + 3]:
                m = _DESC_RE.match(follow)
                if m:
                    desc.append(m.group(1).strip())
            break
    part['description'] = ' '.join(desc)
    for line in lines:
        m = _PRIOR_RE.match(line)
        if m:
            part['prior_req_qty'] = _num(m.group(1))
            part['prior_cum_req_qty'] = _num(m.group(2))
            continue
        m = _LINE_RE.match(line)
        if m:
            part['lines'].append({
                'date': _date(m.group(1)),
                'qty_type': m.group(2),
                'req_qty': _num(m.group(3)),
                'cum_req_qty': _num(m.group(4)),
                'net_req_qty': _num(m.group(5)),
            })
    return part


def parse(text):
    """Texto del release → diccionario. Levanta LearReleaseError si falta algo."""
    if not text or 'MATERIAL RELEASE' not in text.upper():
        raise LearReleaseError("No es un «SUPPLIER SCHEDULE / MATERIAL RELEASE» de Lear.")
    header = {}
    for key, regex in _HEADER_FIELDS.items():
        m = regex.search(text)
        if not m:
            raise LearReleaseError("Falta «%s» en el encabezado." % key)
        header[key] = m.group(1)
    merged = {}
    for block in _pages(text):
        part = _parse_block(block)
        key = (part.get('release_ref'), part.get('customer_part'))
        if key in merged:
            # Página repetida de la misma parte: se agregan sus filas y los
            # pies (autorizaciones) que traiga.
            target = merged[key]
            target['lines'].extend(part['lines'])
            for field, value in part.items():
                if field != 'lines' and value and not target.get(field):
                    target[field] = value
        else:
            merged[key] = part
    if not merged:
        raise LearReleaseError("El release no trae ninguna parte («Release ID»).")
    parts = []
    for part in merged.values():
        for required in ('release_ref', 'release_date', 'customer_part', 'uom',
                         'customer_po'):
            if not part.get(required):
                raise LearReleaseError(
                    "La parte %s no trae «%s»." % (part.get('customer_part') or '?', required))
        if not part['lines']:
            raise LearReleaseError(
                "La parte %s no trae filas semanales." % part['customer_part'])
        dates = [line['date'] for line in part['lines']]
        if len(set(dates)) != len(dates):
            raise LearReleaseError(
                "La parte %s repite semanas: el texto se leyó mal." % part['customer_part'])
        part['lines'].sort(key=lambda line: line['date'])
        parts.append(part)
    header['parts'] = parts
    return header
