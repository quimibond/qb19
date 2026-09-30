# -*- coding: utf-8 -*-
"""Pytest puro del lector del release SUM de FXI.

El Excel se arma aquí con el layout real (reporte SAP volcado) y datos
inventados: no hay archivo del cliente en el repo."""
import io
import os
import sys
from datetime import date, timedelta

import openpyxl
import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

fxi = load('fxi_sum')

HEADER = [None, None, 'Raw Material No.', 'Raw Material Desc', 'Item No.',
          'Vendor Authorization.', None, 'Raw Accum Rcvd', 'Plant', 'Release #',
          'Release Date', 'Agreement #', 'UOM', 'Weeks 27-52']
MONDAYS = [date(2027, 1, 4) + timedelta(weeks=w) for w in range(52)]


def _rows(parts=(('9000001', '64" ZZT038Q46JNG163 PRUEBA (163 CM)'),
                 ('9000002', '64" ZZT046Q46JNG163 PRUEBA (163 CM)'))):
    rows = [
        ['01/01/2027', 'Dynamic List Display', '1'],
        ['Summary Report'],
        ['Vendor:', '999999'],
        ['Vendor Address:', 'PROVEEDOR DE PRUEBA'],
        ['FXI Releases are "Delivery Releases" ...'],
        ['-' * 20],
    ]
    for n, (code, desc) in enumerate(parts, start=1):
        rows.append(list(HEADER))
        rows.append([None, None, code, desc, n * 10, '"12,000"', None, 30000,
                     1500, 29999, '20270101', '5599999999', 'LY', 9999])
        rows.append([None, 'Date'] + ['%d/%d/%02d' % (d.month, d.day, d.year % 100)
                                      for d in MONDAYS])
        rows.append([None, 'Gross need'] + [1000 if w % 2 == 0 else '"1,500"'
                                            for w in range(52)])
        rows.append([None, 'V Cum'] + [31000 + 1250 * w for w in range(52)])
    rows.append([None, '**'])
    return rows


def test_parse_rows_two_parts():
    data = fxi.parse_rows(_rows())
    assert data['vendor'] == '999999'
    assert [p['customer_part'] for p in data['parts']] == ['9000001', '9000002']
    part = data['parts'][0]
    assert part['uom'] == 'LY'
    assert part['release_ref'] == '29999'
    assert part['release_date'] == date(2027, 1, 1)
    assert part['agreement'] == '5599999999'
    assert part['vendor_auth_qty'] == 12000
    assert part['raw_accum_rcvd'] == 30000
    assert 'ZZT038Q46JNG163' in part['description']
    assert len(part['lines']) == 52
    assert part['lines'][0] == {'date': date(2027, 1, 4), 'gross_need': 1000, 'cum': 31000}
    assert part['lines'][1]['gross_need'] == 1500  # «"1,500"» con comillas y coma


def test_parse_xlsx_roundtrip():
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = 'SUM Prueba 01.04.27'
    for row in _rows():
        sheet.append(row)
    buffer = io.BytesIO()
    workbook.save(buffer)
    data = fxi.parse_xlsx(buffer.getvalue())
    assert len(data['parts']) == 2
    assert data['parts'][1]['lines'][-1]['date'] == MONDAYS[-1]


def test_missing_uom_is_rejected():
    rows = _rows(parts=(('9000001', 'X'),))
    rows[7][12] = None
    with pytest.raises(fxi.FxiReleaseError):
        fxi.parse_rows(rows)


def test_not_fxi():
    with pytest.raises(fxi.FxiReleaseError):
        fxi.parse_rows([['Hola'], ['mundo']])
    with pytest.raises(fxi.FxiReleaseError):
        fxi.parse_xlsx(b'no es excel')
