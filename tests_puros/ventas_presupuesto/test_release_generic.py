# -*- coding: utf-8 -*-
"""Pytest puro del lector genérico de releases (tabla parte × fecha, Excel o CSV). Datos inventados."""
import io
import os
import sys
from datetime import date, datetime

import openpyxl
import pytest

sys.path.insert(0, os.path.dirname(__file__))
from release_loader import load  # noqa: E402

gt = load('generic_table')


def test_wide_table_with_dates_across():
    rows = [
        ['RELEASE QUIMIBOND', None, None, None],
        ['Part Number', 'Description', datetime(2040, 1, 9), datetime(2040, 1, 16), datetime(2040, 1, 23)],
        ['LX-100', 'SCRIM 160', 1000, None, '2,500'],
        ['LX-100', 'SCRIM 160', 500, 0, None],
        ['LX-200', 'BACK', 0, 0, 0],
        ['TOTAL', None, 1500, 0, 2500],
    ]
    result = gt.parse_rows(rows, desc_header='Description')
    assert result['layout'] == 'ancha'
    assert [p['customer_part'] for p in result['parts']] == ['LX-100'], "Sin cantidades no es parte; TOTAL no."
    part = result['parts'][0]
    assert part['description'] == 'SCRIM 160'
    assert part['lines'] == [{'date': date(2040, 1, 9), 'qty': 1500.0}, {'date': date(2040, 1, 23), 'qty': 2500.0}]


def test_wide_text_dates_and_order():
    rows = [['Parte', '01/02/40', '08/02/40'], ['P-1', 10, 20]]
    dmy = gt.parse_rows(rows)['parts'][0]['lines']
    assert dmy[0]['date'] == date(2040, 2, 1)
    mdy = gt.parse_rows(rows, order='mdy')['parts'][0]['lines']
    assert mdy[0]['date'] == date(2040, 1, 2)


def test_long_table_part_date_qty():
    rows = [
        ['Material', 'PO', 'Delivery Date', 'Quantity', 'UoM'],
        ['ZW-1', '14792', '2040-03-05', '300', 'M'],
        ['ZW-1', '14792', '2040-03-12', 'pendiente', 'M'],
        ['ZW-2', '14792', '2040-03-05', 0, 'M'],
    ]
    result = gt.parse_rows(rows, po_header='PO')
    assert result['layout'] == 'larga'
    assert result['parts'] == [{'customer_part': 'ZW-1', 'description': None, 'customer_po': '14792',
                                'lines': [{'date': date(2040, 3, 5), 'qty': 300.0}]}]
    assert any('pendiente' in w for w in result['warnings'])


def test_part_header_from_profile_wins():
    rows = [['Item', 'Cust Part', '2040-01-09', '2040-01-16'], ['111', 'ABC-9', 5, 6]]
    assert gt.parse_rows(rows, part_header='Cust Part')['parts'][0]['customer_part'] == 'ABC-9'


def test_no_table_is_an_error():
    with pytest.raises(gt.GenericReleaseError):
        gt.parse_rows([['hola'], ['mundo']])


def test_csv_semicolon_and_xlsx_files():
    csv_bytes = "Part;10/01/2040;17/01/2040\r\nS-1;100;200\r\n".encode('latin-1')
    lines = gt.parse_file(csv_bytes, 'shawmut.csv')['parts'][0]['lines']
    assert [l['qty'] for l in lines] == [100.0, 200.0]
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(['Part', datetime(2040, 1, 9), datetime(2040, 1, 16)])
    ws.append(['X-1', 7, 8])
    buffer = io.BytesIO()
    wb.save(buffer)
    assert gt.parse_file(buffer.getvalue(), 'r.xlsx')['parts'][0]['customer_part'] == 'X-1'
    with pytest.raises(gt.GenericReleaseError):
        gt.parse_file(b'\xd0\xcf\x11\xe0', 'viejo.xls')


def test_shawmut_silao_header_in_one_cell_by_column_number():
    # Mismo acomodo que «SL 3254 Release for W45 2026.csv» (valores inventados).
    text = ("Plant / Program End Date / Transit Qty / Release / Item / Desc / Greige / On-Hand Qty / Ordered CYTD / "
            "Received CYTD / Mat Auth / Fab Auth / Weight Conv,,,,,,,,,,,,,11/2/26,11/9/26,11/16/26\r\n"
            "12, ,0,0000999,900001,ZZJ042Q22JNT160 63,,100.5,1000,1000,2000,2000,0,240,,15\r\n"
            ",,,,900002,ZZJ038Q22JNT160 63,,50,10,10,20,20,0,,,\r\n")
    result = gt.parse_file(text.encode(), 'SL 3254 Release for W45 2026.csv', order='mdy', part_col=4, desc_col=5)
    assert [p['customer_part'] for p in result['parts']] == ['900001']
    part = result['parts'][0]
    assert part['description'] == 'ZZJ042Q22JNT160 63'
    assert part['lines'] == [{'date': date(2026, 11, 2), 'qty': 240.0}, {'date': date(2026, 11, 16), 'qty': 15.0}]


def test_numeric_text_columns_are_not_the_part():
    rows = [['Plant', 'Item', '2040-01-09', '2040-01-16'], ['12', 'ABC', '5', '6']]
    assert gt.parse_rows(rows)['parts'][0]['customer_part'] == 'ABC'
