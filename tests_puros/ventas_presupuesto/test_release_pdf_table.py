# -*- coding: utf-8 -*-
"""Pytest puro del lector de PDF con tabla parte × fecha (3.27.0, COPO). El texto es el que da pypdf en modo
«layout» con la estructura real del PDF; partes, OC, facturas y contacto inventados."""
import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

pt = load('pdf_table')
gt = load('generic_table')

with open(os.path.join(os.path.dirname(__file__), 'fixtures', 'release_pdf_tabla_layout.txt'),
          encoding='utf-8') as fh:
    LAYOUT = fh.read()


def test_fechas_con_mes_en_letra():
    assert gt.to_date('16-Nov-26') == date(2026, 11, 16)
    assert gt.to_date('7-dic-2026') == date(2026, 12, 7)
    assert gt.to_date('4-Jan-27') == date(2027, 1, 4)
    assert gt.to_date('Release') is None


def test_tabla_por_posicion():
    data = pt.parse_text(LAYOUT, po_header='Purchase Order Number', desc_header='Part Description')
    assert data['release_ref'] == '47'
    assert data['release_date'] == date(2026, 11, 16)
    first, second = data['parts']
    assert (first['customer_part'], first['customer_po']) == ('900110', '91000001')
    assert first['description'] == 'TELA 100%PES 53GR WH 1600MM', "La descripción en dos renglones se junta"
    assert first['last_invoice'] == 'INV2040/10/0188'
    assert first['cum_received'] == 8141.0
    # Las semanas vacías no recorren las cantidades.
    assert [(ln['date'], ln['qty']) for ln in first['lines']] == [
        (date(2026, 11, 16), 18000.0), (date(2027, 1, 4), 10000.0), (date(2027, 1, 25), 12000.0),
        (date(2027, 2, 15), 13000.0), (date(2027, 3, 8), 5000.0), (date(2027, 3, 29), 8000.0)]
    assert second['customer_part'] == '900121'
    assert second['description'] == 'TELA 100%PA 40GR 1630MM', "Las notas al pie no se pegan a la última fila"
    assert [(ln['date'], ln['qty']) for ln in second['lines']][:2] == [
        (date(2026, 11, 16), 23410.0), (date(2026, 11, 23), 11590.0)]


def test_sin_fechas_no_es_tabla():
    with pytest.raises(pt.PdfTableError):
        pt.parse_text("Release No. 1\nPart  Qty\nA  10\n")


# 3.27.1: Odoo.sh trae un pypdf sin modo «layout»; el acomodo se arma con las posiciones del content stream.
STREAM = b"""BT /F1 3.48 Tf 1 0 0 1 119.4 563.16 Tm [(Rel)-16(ease No)4(.)] TJ ET
BT /F1 3.48 Tf 1 0 0 1 179.66 563.16 Tm (47) Tj ET
BT /F1 3.48 Tf 1 0 0 1 117 549.84 Tm (Date Release) Tj ET
BT /F1 3.48 Tf 1 0 0 1 166.1 549.84 Tm (16/11/2026) Tj ET
BT /F1 3.48 Tf 1 0 0 1 50.4 527.88 Tm (CTMX) Tj ET
BT /F1 3.48 Tf 1 0 0 1 79.2 525.72 Tm (Part Description) Tj ET
BT /F1 3.48 Tf 1 0 0 1 112.3 525.72 Tm (Last Invoice considered) Tj ET
BT /F1 3.48 Tf 1 0 0 1 164.8 525.72 Tm (Purchase Order Number) Tj ET
BT /F1 3.48 Tf 1 0 0 1 225.4 525.72 Tm (Llegadas CTM) Tj ET
BT /F1 3.48 Tf 1 0 0 1 260.7 525.72 Tm (16-Nov-26) Tj ET
BT /F1 3.48 Tf 1 0 0 1 287.6 525.72 Tm (23-Nov-26) Tj ET
BT /F1 3.48 Tf 1 0 0 1 312.9 525.72 Tm (30-Nov-26) Tj ET
BT /F1 3.48 Tf 1 0 0 1 45.8 523.56 Tm (Part Number) Tj ET
BT /F1 2.64 Tf 1 0 0 1 74.4 516.12 Tm (TELA 100%PES 53GR WH) Tj ET
q 1 0 0 1 0 0 cm BT /F1 3.48 Tf 1 0 0 1 174.4 514.56 Tm (91000001) Tj ET Q
BT /F1 3.48 Tf 1 0 0 1 49 514.44 Tm (900110) Tj ET
BT /F1 3.48 Tf 1 0 0 1 115.6 514.44 Tm (INV2040/10/0188) Tj ET
BT /F1 3.48 Tf 1 0 0 1 231.5 514.44 Tm (8141) Tj ET
BT /F1 3.48 Tf 1 0 0 1 262.9 514.44 Tm (18000) Tj ET
BT /F1 3.48 Tf 1 0 0 1 314.9 514.44 Tm (5000) Tj ET
BT /F1 2.64 Tf 1 0 0 1 85.4 512.76 Tm (1600MM) Tj ET
BT /F1 3.48 Tf 1 0 0 1 49 507.96 Tm (900121) Tj ET
BT /F1 2.64 Tf 1 0 0 1 72.6 507.96 Tm <54454C41203130302550412034304752> Tj ET
BT /F1 3.48 Tf 1 0 0 1 115.6 507.96 Tm (INV2040/11/0006) Tj ET
BT /F1 3.48 Tf 1 0 0 1 174.4 507.96 Tm (91000002) Tj ET
BT /F1 3.48 Tf 1 0 0 1 230.5 507.96 Tm (19274) Tj ET
BT /F1 3.48 Tf 1 0 0 1 289.7 507.96 Tm (11590) Tj ET
BT /F1 2.88 Tf 1 0 0 1 69.8 497.86 Tm (Las fechas mostradas son en las cuales el material debe estar listo) Tj ET
"""


def test_acomodo_desde_el_content_stream():
    text = pt.stream_layout_text(STREAM)
    data = pt.parse_text(text, po_header='Purchase Order Number', desc_header='Part Description')
    assert (data['release_ref'], data['release_date']) == ('47', date(2026, 11, 16))
    first, second = data['parts']
    assert (first['customer_part'], first['customer_po']) == ('900110', '91000001')
    assert first['description'] == 'TELA 100%PES 53GR WH 1600MM'
    assert first['last_invoice'] == 'INV2040/10/0188'
    assert first['cum_received'] == 8141.0
    assert [(ln['date'], ln['qty']) for ln in first['lines']] == [
        (date(2026, 11, 16), 18000.0), (date(2026, 11, 30), 5000.0)]
    assert (second['customer_part'], second['description']) == ('900121', 'TELA 100%PA 40GR')
    assert [(ln['date'], ln['qty']) for ln in second['lines']] == [(date(2026, 11, 23), 11590.0)]


class _OldPage:
    """Página de un pypdf viejo / PyPDF2: sin modo layout y con getContents()."""

    def extract_text(self):
        return ''

    def getContents(self):
        return self

    def getData(self):
        return STREAM


class _OldReader:
    def __init__(self, _stream):
        self.pages = [_OldPage()]


def test_pypdf_sin_modo_layout():
    data = pt.parse_pdf(b'%PDF', _OldReader, po_header='Purchase Order Number', desc_header='Part Description')
    assert [p['customer_part'] for p in data['parts']] == ['900110', '900121']
