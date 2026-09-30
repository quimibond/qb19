# -*- coding: utf-8 -*-
"""Pytest puro del lector del release de Lear (texto AIAG).

El fixture es inventado con el layout real (el release de 24-sep-26 se vio
en el correo; aquí no hay cifras ni partes reales)."""
import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

lear = load('lear_aiag')
FIXTURE = os.path.join(os.path.dirname(__file__), 'fixtures', 'lear_release_ficticio.txt')


def _text():
    with open(FIXTURE, encoding='utf-8') as fh:
        return fh.read()


def test_header_and_part():
    data = lear.parse(_text())
    assert data['supplier'] == '9ZZZ0001' and data['ship_to'] == '9900'
    assert len(data['parts']) == 1
    part = data['parts'][0]
    assert part['release_ref'] == '000901'
    assert part['release_date'] == date(2027, 1, 7)
    assert part['customer_po'] == '7000001'
    assert part['customer_part'] == 'LX00000001AA'
    assert part['uom'] == 'MT'
    assert part['description'] == 'BACK SCRIM PES/100 IH LAMINATED 62"'
    assert part['in_transit_qty'] == 1200
    assert part['cum_received'] == 50000
    assert part['last_receipt_date'] == date(2027, 1, 4)
    assert part['packing_slip'] == 'INV/2027/01/0001'
    assert part['fab_auth_qty'] == 56000 and part['fab_auth_date'] == date(2027, 1, 10)
    assert part['raw_auth_qty'] == 61000 and part['raw_auth_date'] == date(2027, 1, 31)
    assert part['prior_cum_req_qty'] == 50000


def test_weekly_lines_across_pages_and_year():
    part = lear.parse(_text())['parts'][0]
    assert [line['date'] for line in part['lines']] == [
        date(2026, 12, 28), date(2027, 1, 4), date(2027, 1, 11),
        date(2027, 1, 18), date(2027, 1, 25)]
    assert part['lines'][0] == {'date': date(2026, 12, 28), 'qty_type': 'P',
                                'req_qty': 3000, 'cum_req_qty': 53000,
                                'net_req_qty': 1800}
    # El acumulado cuadra: cum recibido + Σ req = último cum req.
    assert part['cum_received'] + sum(line['req_qty'] for line in part['lines']) \
        == part['lines'][-1]['cum_req_qty']


def test_not_a_release():
    with pytest.raises(lear.LearReleaseError):
        lear.parse("Hola, adjunto factura")


def test_missing_po_is_rejected():
    text = _text().replace('Purchase Order: 7000001', 'Purchase Order:')
    with pytest.raises(lear.LearReleaseError):
        lear.parse(text)


def test_duplicated_week_is_rejected():
    text = _text().replace('01/25/27 P 0 61,000 0', '01/18/27 P 0 61,000 0')
    with pytest.raises(lear.LearReleaseError):
        lear.parse(text)
