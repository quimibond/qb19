# -*- coding: utf-8 -*-
"""Lector del release de Woodbridge (Saltillo Lamination, plantas Saltillo y León), 19.0.3.16.0.

Llega en PDF: «130551 - Quimibond MM-DD-YYYY D.pdf» (Saltillo) y «Release Quimibond WKnn.D.pdf»
(León). Es el «SUPPLIER RELEASE PLANNING SCHEDULE» de Woodbridge, una página por parte:

    SUPPLIER RELEASE PLANNING SCHEDULE Blanket P/O #: 6700 Release#: 2026306C Date: 11/02/26
    Vendor: 130551 Ship To: …
    Part #: 290524 Vendor Part #: 290524 MOQ 500 MTR Model Year
    Quimibond 60gsm CKS Standard Package: 1.00000 MTR
    MOQ 500 MTR Last Receipt: 1918.00000 9/24/26 SID: INV/2026/09/0134
    Cum YTD Required: 33381.00000
    Cum YTD Received: 33381.00000
    Requirements Unit: MTR
    Delivery Net Cum Previous Net
    Date Period Authorization Quantity Quantity Release Change
    11/23/26 Week Firm 527.00 33908.00000 157.27 369.73
    …
    Fabrication Cum: 36668.00000 Material Cum: 36668.00000

Fechas mm/dd/aa de **entrega** en la planta del cliente. La OC es la «Blanket P/O» (6700
Saltillo, 6600319 León) y el número de proveedor dice la planta (130551 Saltillo, 130551L
León). El PDF de León arrastra a veces una página vieja (2023) de una parte ya sin uso: se
descartan las páginas más de 180 días anteriores a la más reciente.

Python puro (recibe el texto del PDF): lo prueban ``tests_puros/ventas_presupuesto/
test_release_woodbridge.py`` sin Odoo.

Salida (igual que los otros lectores):
    {'vendor': '130551L', 'parts': [{'customer_part', 'description', 'customer_po',
      'release_ref', 'release_date', 'uom', 'cum_received', 'fab_cum',
      'lines': [{'date', 'qty', 'authorization', 'cum'}]}]}
"""
import re
from datetime import date, timedelta


class WoodbridgeReleaseError(ValueError):
    """El texto no es un release de Woodbridge legible."""


_PAGE = re.compile(r'SUPPLIER\s+RELEASE\s+PLANNING\s+SCHEDULE', re.IGNORECASE)
_HEADER = re.compile(r'Blanket\s*P/O\s*#?:\s*(?P<po>\S+)\s+Release\s*#?:\s*(?P<rel>\S+)\s+'
                     r'Date:\s*(?P<date>\d{1,2}/\d{1,2}/\d{2,4})', re.IGNORECASE)
_VENDOR = re.compile(r'Vendor:\s*(?P<vendor>[0-9A-Z]+)', re.IGNORECASE)
_PART = re.compile(r'^\s*Part\s*#:\s*(?P<part>\S+)\s+Vendor\s+Part\s*#:\s*(?P<vpart>.*?)\s*(?:Model\s+Year.*)?$',
                   re.IGNORECASE)
_UNIT = re.compile(r'Requirements\s+Unit:\s*(?P<uom>[A-Z0-9]+)', re.IGNORECASE)
_CUM_RCVD = re.compile(r'Cum\s+YTD\s+Received:\s*(?P<v>[\d,]*\.?\d+)', re.IGNORECASE)
_FAB_CUM = re.compile(r'Fabrication\s+Cum:\s*(?P<v>[\d,]*\.?\d+)', re.IGNORECASE)
_LINE = re.compile(r'^\s*(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s+(?P<period>Week|Day|Month)\s+'
                   r'(?P<auth>[A-Za-z]+)\s+(?P<qty>-?[\d,]*\.?\d+)\s+(?P<cum>[\d,]*\.?\d+)', re.IGNORECASE)
_DESC_END = re.compile(r'\s*Standard\s+Package:.*$', re.IGNORECASE)
_RECEIPT = re.compile(r'\s*Last\s+Receipt:.*$', re.IGNORECASE)
STALE_DAYS = 180


def _num(text):
    return float((text or '0').replace(',', ''))


def _date(text):
    month, day, year = (int(x) for x in text.split('/'))
    if year < 100:
        year += 2000
    return date(year, month, day)


def _parse_page(text):
    header = _HEADER.search(text)
    if not header:
        return None
    lines = text.splitlines()
    part = description = None
    for index, line in enumerate(lines):
        match = _PART.match(line)
        if match:
            part = match.group('part').strip()
            vendor_part = match.group('vpart').strip()
            extra = []
            for follow in lines[index + 1:index + 3]:
                follow = _RECEIPT.sub('', _DESC_END.sub('', follow)).strip()
                if follow and not follow.lower().startswith('cum '):
                    extra.append(follow)
            description = " ".join([vendor_part] + extra).strip() or None
            break
    unit = _UNIT.search(text)
    if not part or not unit:
        raise WoodbridgeReleaseError("Página del release %s sin «Part #» o sin «Requirements Unit»."
                                     % header.group('rel'))
    weeks = []
    for line in lines:
        match = _LINE.match(line)
        if match:
            weeks.append({'date': _date(match.group('date')), 'qty': _num(match.group('qty')),
                          'authorization': match.group('auth').capitalize(), 'cum': _num(match.group('cum'))})
    cum = _CUM_RCVD.search(text)
    fab = _FAB_CUM.search(text)
    vendor = _VENDOR.search(text)
    return {
        'vendor': vendor.group('vendor').upper() if vendor else None,
        'customer_part': part, 'description': description, 'customer_po': header.group('po'),
        'release_ref': header.group('rel'), 'release_date': _date(header.group('date')),
        'uom': unit.group('uom').upper(), 'cum_received': _num(cum.group('v')) if cum else 0.0,
        'fab_cum': _num(fab.group('v')) if fab else 0.0, 'lines': weeks,
    }


def parse_text(text):
    """Texto del PDF → {'vendor', 'parts'}. Levanta WoodbridgeReleaseError si no hay ninguna parte."""
    pages = [p for p in (_parse_page(chunk) for chunk in _PAGE.split(text or '')) if p]
    if not pages:
        raise WoodbridgeReleaseError("No es un «Supplier Release Planning Schedule» de Woodbridge.")
    newest = max(p['release_date'] for p in pages)
    pages = [p for p in pages if p['release_date'] >= newest - timedelta(days=STALE_DAYS)]
    vendors = {p['vendor'] for p in pages if p['vendor']}
    if len(vendors) > 1:
        raise WoodbridgeReleaseError("El archivo trae varias plantas (%s): súbalas por separado."
                                     % ", ".join(sorted(vendors)))
    return {'vendor': vendors.pop() if vendors else None, 'parts': pages}


def pdf_text(content, reader_cls):
    """Texto de un PDF con el lector que trae Odoo (pypdf / PyPDF2); ``reader_cls`` se inyecta para
    no depender de él aquí."""
    import io
    reader = reader_cls(io.BytesIO(content))
    chunks = []
    for page in reader.pages:
        extract = getattr(page, 'extract_text', None) or getattr(page, 'extractText', None)
        chunks.append(extract() or '')
    return "\n".join(chunks)
