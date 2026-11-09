# -*- coding: utf-8 -*-
"""Pytest puro del lector de Woodbridge (Saltillo Lamination). Texto con la estructura real del PDF
(«Supplier Release Planning Schedule»); partes, OC y cantidades inventadas."""
import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

wb = load('woodbridge_pdf')

SALTILLO = """SUPPLIER RELEASE PLANNING SCHEDULE Blanket P/O #: 9100 Release#: 2040306C Date: 11/02/40
Vendor: 999001 Ship To:
CLIENTE DE PRUEBA SA DE CV
Part #: 111111 Vendor Part #: 111111 MOQ 500 MTR Model Year
Tela prueba 60gsm Standard Package: 1.00000 MTR
MOQ 500 MTR Last Receipt: 100.00000 9/24/40 SID: INV/2040/09/0001
Cum YTD Required: 1000.00000
Cum YTD Received: 1,000.00000
Requirements Unit: MTR
Delivery Net Cum Previous Net
Date Period Authorization Quantity Quantity Release Change
11/05/40 Week Firm .00 1000.00000 .00 .00
11/12/40 Week Firm 527.00 1527.00000 157.27 369.73
11/19/40 Week Forecast 1,172.50 2699.50000 893.30 279.20
Fabrication Cum: 2699.50000 Material Cum: 2699.50000
SUPPLIER RELEASE PLANNING SCHEDULE Blanket P/O #: 9100 Release#: 2040306C Date: 11/02/40
Vendor: 999001 Ship To:
Part #: 222222 Vendor Part #: WJ032Q22JNT160 Model Year
Tela prueba 32gsm Standard Package: 1.00000 YD
MOQ 6000 YD Last Receipt: 10.00000 10/22/40 SID: INV/2040/10/0002
Cum YTD Required: 50.00000
Cum YTD Received: 50.00000
Requirements Unit: YD
Delivery Net Cum Previous Net
Date Period Authorization Quantity Quantity Release Change
11/26/40 Week Forecast 4481.00 4531.00000 6492.00 2011.00-
Fabrication Cum: 50.00000 Material Cum: 50.00000
"""

LEON_CON_PAGINA_VIEJA = """SUPPLIER RELEASE PLANNING SCHEDULE Blanket P/O #: 9200 Release#: 2023205 Date: 7/24/38
Vendor: 999001L Ship To:
Part #: 333333L Vendor Part #: WJ053Q22JNT160 (63 ) Model Year
Tela vieja Standard Package: 1.00000 LY
Requirements Unit: LY
7/24/38 Week Firm .00 1452.31800 .00 .00
SUPPLIER RELEASE PLANNING SCHEDULE Blanket P/O #: 9200 Release#: 2040307C Date: 11/03/40
Vendor: 999001L Ship To:
Part #: 444444L Vendor Part #: Model Year
Circular Knit Quimibond WJ60Q2 Standard Package: 1.00000 MTR
1JNT160 (63") Last Receipt: 2468.00000 10/14/40 SID: INV/2040/09/0202
Cum YTD Received: 89716.00000
Requirements Unit: MTR
11/05/40 Week Firm 844.24 90560.24000 844.24 .00
11/12/40 Week Fabrication 684.00 91244.24000 684.00 .00
Fabrication Cum: 92270.24000 Material Cum: 92270.24000
"""


def test_saltillo_lee_partes_oc_unidad_y_semanas():
    data = wb.parse_text(SALTILLO)
    assert data['vendor'] == '999001'
    first, second = data['parts']
    assert (first['customer_part'], first['customer_po'], first['release_ref']) == ('111111', '9100', '2040306C')
    assert first['release_date'] == date(2040, 11, 2)
    assert first['uom'] == 'MTR'
    assert first['cum_received'] == 1000.0
    assert [(ln['date'], ln['qty'], ln['authorization']) for ln in first['lines']] == [
        (date(2040, 11, 5), 0.0, 'Firm'), (date(2040, 11, 12), 527.0, 'Firm'),
        (date(2040, 11, 19), 1172.5, 'Forecast')]
    assert 'Tela prueba 60gsm' in first['description']
    assert second['uom'] == 'YD'
    assert second['description'].startswith('WJ032Q22JNT160')
    assert second['lines'][0]['qty'] == 4481.0


def test_leon_descarta_la_pagina_vieja():
    data = wb.parse_text(LEON_CON_PAGINA_VIEJA)
    assert data['vendor'] == '999001L'
    assert [p['customer_part'] for p in data['parts']] == ['444444L']
    part = data['parts'][0]
    assert part['customer_po'] == '9200'
    assert [ln['authorization'] for ln in part['lines']] == ['Firm', 'Fabrication']


def test_dos_plantas_en_un_archivo_no_se_leen():
    with pytest.raises(wb.WoodbridgeReleaseError):
        leon_actual = LEON_CON_PAGINA_VIEJA.split('SUPPLIER RELEASE PLANNING SCHEDULE')[2]
        wb.parse_text(SALTILLO + 'SUPPLIER RELEASE PLANNING SCHEDULE' + leon_actual)


def test_texto_que_no_es_release():
    with pytest.raises(wb.WoodbridgeReleaseError):
        wb.parse_text("Factura cualquiera")
