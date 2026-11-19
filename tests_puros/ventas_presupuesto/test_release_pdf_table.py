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
